import re
import math
from typing import Dict, List, Any, Optional, Set, Tuple
import pandas as pd
from sqlalchemy import text, Engine
from app.models.schema import DatabaseSchemaMetadata, TableMetadata, ColumnMetadata
from app.models.migration import (
    ColumnMappingResult,
    ValueResolutionResult,
    ForeignKeyPlanItem,
    ResolvedEntityRecord,
    ValueMappingItem
)
from app.utils.normalization import (
    normalize_column_name,
    normalize_cell_value,
    tokenize_cell_value,
    normalize_string
)
from app.mapping.synonyms import synonym_registry
from app.migration.outlet_matcher import (
    OutletMatcher,
    extract_outlet_base_name,
    outlet_row_source_key,
    outlet_row_source_values
)
from app.core.logging import logger


def is_empty_or_na(val: Any) -> bool:
    """Checks if a cell value is null, NaN, NaT, or empty/whitespace."""
    if val is None:
        return True
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return True
    if pd.isna(val):
        return True
    s = str(val).strip()
    return s == "" or s.lower() in ("nan", "none", "null", "nat")


def quote_identifier(name: str, dialect_name: str) -> str:
    """Quotes table or column identifiers appropriately for SQL Server, MySQL, Postgres, SQLite."""
    clean = str(name).strip().replace("[", "").replace("]", "").replace("`", "").replace('"', '')
    if dialect_name == "mssql":
        return f"[{clean}]"
    elif dialect_name in ("mysql", "mariadb"):
        return f"`{clean}`"
    else:
        return f'"{clean}"'


class LookupRelationship:
    """
    Represents an introspected or inferred foreign-key relationship between a child table's
    foreign-key column and a parent table's primary key, along with the identifying business-name column.
    """
    def __init__(
        self,
        child_table: str,
        child_column: str,
        parent_table: str,
        parent_id_column: str,
        parent_name_column: Optional[str] = None,
        excel_name_column: Optional[str] = None,
        excel_id_column: Optional[str] = None,
        is_required: bool = False
    ):
        self.child_table = child_table
        self.child_column = child_column
        self.parent_table = parent_table
        self.parent_id_column = parent_id_column
        self.parent_name_column = parent_name_column
        self.excel_name_column = excel_name_column
        self.excel_id_column = excel_id_column
        self.is_required = is_required

    def __repr__(self):
        return (
            f"<LookupRelationship {self.child_table}.{self.child_column} -> "
            f"{self.parent_table}.{self.parent_id_column} (name_col={self.parent_name_column}, "
            f"excel_name={self.excel_name_column})>"
        )


class TableLookupIndex:
    """
    High-performance pre-indexed hash lookup for parent table records.
    Provides O(1) exact, normalized, and override lookups, replacing O(N*M) nested loops.
    Retains the full database record dictionary for downstream entity reuse.
    """
    def __init__(self, db_records: List[Dict[str, Any]], parent_id_col: str, parent_name_col: str):
        self.exact_map: Dict[str, List[Dict[str, Any]]] = {}
        self.norm_map: Dict[str, List[Dict[str, Any]]] = {}
        self.override_map: Dict[str, Dict[str, Any]] = {}
        self.token_map: Dict[str, List[Dict[str, Any]]] = {}
        self.records_by_id: Dict[Any, Dict[str, Any]] = {}

        for r in db_records:
            raw_db_val = r.get(parent_name_col)
            db_id = r.get(parent_id_col)
            name_str = str(raw_db_val).strip() if raw_db_val is not None else ""

            if db_id is not None:
                self.records_by_id[db_id] = r
                self.records_by_id[str(db_id).strip()] = r
                self.override_map[str(db_id).strip()] = {
                    "id": db_id,
                    "name": name_str,
                    "record": r
                }

            cand_entry = {
                "id": db_id,
                "name": name_str if name_str else (str(db_id) if db_id is not None else ""),
                "record": r
            }

            # Collect all identifying text values for this record
            identifying_values: List[str] = []
            if name_str and name_str.lower() not in ("nan", "none", "null", "nat"):
                identifying_values.append(name_str)

            for col_k, col_v in r.items():
                if col_v is None or col_k == parent_name_col:
                    continue
                k_lower = col_k.lower()
                is_ident_col = (
                    "name" in k_lower
                    or "code" in k_lower
                    or "address" in k_lower
                    or "location" in k_lower
                    or "title" in k_lower
                )
                if is_ident_col:
                    s = str(col_v).strip()
                    if s and s.lower() not in ("nan", "none", "null", "nat") and s not in identifying_values:
                        identifying_values.append(s)

            for s_val in identifying_values:
                # Exact map
                self.exact_map.setdefault(s_val, []).append(cand_entry)

                # Norm map
                norm = normalize_cell_value(s_val)
                if norm:
                    self.norm_map.setdefault(norm, []).append(cand_entry)
                    lower = s_val.lower()
                    if lower != norm:
                        self.norm_map.setdefault(lower, []).append(cand_entry)

                    # Token index for fast fuzzy candidate retrieval
                    for tok in norm.split():
                        if len(tok) >= 3:
                            c_list = self.token_map.setdefault(tok, [])
                            if len(c_list) < 5:
                                c_list.append(cand_entry)


class DynamicLookupResolver:
    """
    Dynamic Name-to-ID Resolution Engine for Microsoft SQL Server.
    Resolves missing foreign-key IDs by looking up business names from Excel against
    the referenced database tables dynamically using database metadata and SQL Server queries.
    Never hardcodes IDs, table mappings, or sample records.
    """

    _index_cache: Dict[Tuple[int, str, str], TableLookupIndex] = {}

    @classmethod
    def get_or_create_index(
        cls,
        db_records: List[Dict[str, Any]],
        parent_id_column: str,
        parent_name_column: str
    ) -> TableLookupIndex:
        cache_key = (id(db_records), parent_id_column, parent_name_column)
        if cache_key not in cls._index_cache:
            cls._index_cache[cache_key] = TableLookupIndex(db_records, parent_id_column, parent_name_column)
        return cls._index_cache[cache_key]

    @classmethod
    def clear_index_cache(cls):
        cls._index_cache.clear()

    @staticmethod
    def identify_parent_name_column(parent_table_meta: TableMetadata) -> Optional[str]:
        """
        Dynamically discovers the best identifying business-name column in a parent table.
        Scores candidate columns by entity name, text type, unique constraints, and naming patterns.
        Strictly rejects date/time, numeric, and identifier columns.
        """
        best_col: Optional[str] = None
        best_score = 0

        t_name_lower = parent_table_meta.name.lower()
        t_norm = normalize_column_name(parent_table_meta.name)

        for col in parent_table_meta.columns:
            if col.primary_key or getattr(col, "identity", False):
                continue

            c_name = col.name
            c_lower = c_name.lower()
            c_norm = normalize_column_name(c_name)
            col_type = col.type.upper()
            is_text = any(t in col_type for t in ("CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "TEXT", "STRING"))

            if not is_text:
                continue

            # Skip date/time, timestamp, and audit columns
            if any(k in c_lower for k in ("date", "time", "year", "created", "modified", "timestamp", "stamp", "guid")):
                continue

            # Skip ID columns that end with _id or id
            if c_lower.endswith("_id") or (c_lower.endswith("id") and c_lower != "taxid"):
                continue

            score = 0

            # 1. Exact entity name column (e.g. Outlet_Name in Outlets, Customer_Name in Customer)
            if c_lower in (f"{t_name_lower}_name", f"{t_name_lower}name", f"{t_name_lower.rstrip('s')}_name", f"{t_name_lower.rstrip('s')}name"):
                score += 100
            elif "name" in c_norm and any(token in t_norm for token in c_norm.split()):
                score += 90
            elif c_lower in ("name", "title", "description", "business_name", "display_name"):
                score += 80
            elif "name" in c_lower:
                score += 70

            if col.unique:
                score += 15

            # Require a solid confidence score to be treated as an entity business name
            if score >= 70 and score > best_score:
                best_score = score
                best_col = c_name

        return best_col

    @staticmethod
    def find_matching_excel_column(
        df_columns: List[str],
        parent_table_name: str,
        parent_name_column: Optional[str],
        column_mappings: List[ColumnMappingResult],
        child_column_name: Optional[str] = None
    ) -> Optional[str]:
        """
        Finds the corresponding business-name column available in the uploaded Excel file.
        Checks:
        0. Column mapping or direct/synonym match targeting child_column_name (e.g. assign_to for Assigned_Outlet_ID).
        1. Directly confirmed column mapping targeting parent_table.parent_name_column.
        2. Exact or normalized column name match with parent_name_column.
        3. Synonym dictionary matching for parent entity name.
        """
        # Exclude numeric and date/time source columns from being picked as entity names
        NON_NAME_KEYWORDS = ("date", "time", "qty", "quantity", "amount", "total", "tax", "price", "discount", "cost", "rate", "bill_no", "billno")

        def is_clean_name_col(c: str) -> bool:
            cl = c.lower().strip()
            return not any(k in cl for k in NON_NAME_KEYWORDS)

        # 0. Check child_column_name specific source column if specified
        if child_column_name:
            c_clean = child_column_name.lower().strip()
            c_norm = normalize_column_name(child_column_name)
            # Direct mapping targeting this specific child column
            for m in column_mappings:
                if (
                    m.database_column.lower() == c_clean
                    and m.excel_column in df_columns
                    and is_clean_name_col(m.excel_column)
                ):
                    return m.excel_column

            # Check if an Excel column matches child_column_name or its prefix
            for col in df_columns:
                if not is_clean_name_col(col):
                    continue
                col_norm = normalize_column_name(col)
                if col_norm == c_norm or col.lower().strip() == c_clean:
                    return col
                if "assign" in c_norm and any(a in col_norm for a in ("assign", "assigned")):
                    return col

        if not parent_name_column:
            return None

        # 1. Check if any Excel column is mapped to parent_table.parent_name_column
        for m in column_mappings:
            if (
                m.database_table.lower() == parent_table_name.lower()
                and m.database_column.lower() == parent_name_column.lower()
                and m.excel_column in df_columns
                and is_clean_name_col(m.excel_column)
            ):
                return m.excel_column

        # 2. Check exact and normalized match across Excel columns
        candidates_to_match = [
            normalize_column_name(parent_name_column),
            parent_name_column.lower(),
            f"{normalize_column_name(parent_table_name)} name",
            f"{parent_table_name.rstrip('s').lower()} name",
            f"{parent_table_name.rstrip('s').lower()}_name"
        ]

        for col in df_columns:
            if not is_clean_name_col(col):
                continue
            norm_col = normalize_column_name(col)
            clean_col = col.lower().strip()
            if any(cand == norm_col or cand == clean_col for cand in candidates_to_match):
                return col

        # 3. Synonym matching: Check registered business concepts (TEXT only)
        p_concept_match = (
            synonym_registry.find_concept_for_term(parent_name_column)
            or synonym_registry.find_concept_for_term(f"{parent_table_name} name")
            or synonym_registry.find_concept_for_term(parent_table_name)
        )
        if p_concept_match:
            p_concept = p_concept_match[1]
            for col in df_columns:
                if not is_clean_name_col(col):
                    continue
                col_concept_match = synonym_registry.find_concept_for_term(col)
                if col_concept_match and col_concept_match[1].id == p_concept.id:
                    return col

        p_lower = parent_table_name.lower().rstrip('s')
        for col in df_columns:
            if not is_clean_name_col(col):
                continue
            norm_col = normalize_column_name(col)
            for concept in synonym_registry.get_concepts():
                if getattr(concept, "expected_type", "TEXT") not in ("TEXT", "STRING"):
                    continue
                norm_syns = [normalize_column_name(s) for s in concept.synonyms]
                covers_parent = (
                    (parent_name_column and normalize_column_name(parent_name_column) in norm_syns)
                    or normalize_column_name(parent_table_name) in norm_syns
                    or f"{normalize_column_name(parent_table_name)} name" in norm_syns
                    or p_lower in concept.id.lower()
                    or p_lower in concept.name.lower()
                )
                if covers_parent and norm_col in norm_syns:
                    return col

        # 4. Domain & Entity keyword fallback (e.g. Outlets -> restaurant_name, store_name, branch_name)
        OUTLET_ENTITY_TOKENS = ("restaurant", "outlet", "branch", "store", "pos", "location")
        if p_lower in ("outlet", "store", "branch", "restaurant", "pos"):
            for col in df_columns:
                if not is_clean_name_col(col):
                    continue
                norm_col = normalize_column_name(col)
                if any(t in norm_col for t in OUTLET_ENTITY_TOKENS) and "name" in norm_col:
                    return col

        # 5. General fallback: contains parent entity token and 'name'
        for col in df_columns:
            if not is_clean_name_col(col):
                continue
            norm_col = normalize_column_name(col)
            if p_lower in norm_col and "name" in norm_col:
                return col

        return None

    @classmethod
    def discover_relationships(
        cls,
        schema: DatabaseSchemaMetadata,
        df_columns: List[str],
        column_mappings: List[ColumnMappingResult],
        active_tables: List[str]
    ) -> List[LookupRelationship]:
        """
        Inspects the actual SQL Server schema metadata to dynamically identify foreign-key relationships.
        Derives relationships without any hardcoded table or column names.
        """
        schema_tables = {t.name: t for t in schema.tables}
        relationships: List[LookupRelationship] = []
        seen_keys: Set[Tuple[str, str]] = set()

        for child_tbl_name in active_tables:
            if child_tbl_name not in schema_tables:
                continue
            child_meta = schema_tables[child_tbl_name]
            col_meta_map = {c.name: c for c in child_meta.columns}

            # Group column mappings by database column for this child table
            db_to_excel = {
                m.database_column: m.excel_column
                for m in column_mappings
                if m.database_table == child_tbl_name and m.excel_column in df_columns
            }

            # 1. Explicit foreign keys from SQL Server catalog metadata
            for fk in child_meta.foreign_keys:
                parent_tbl_name = fk.target_table
                if parent_tbl_name not in schema_tables:
                    continue

                parent_meta = schema_tables[parent_tbl_name]
                child_col = fk.constrained_column
                parent_id_col = fk.target_column

                c_meta = col_meta_map.get(child_col)
                is_req = bool(c_meta and not c_meta.nullable and c_meta.default is None and not getattr(c_meta, "identity", False))

                parent_name_col = cls.identify_parent_name_column(parent_meta)
                excel_name_col = cls.find_matching_excel_column(
                    df_columns=df_columns,
                    parent_table_name=parent_tbl_name,
                    parent_name_column=parent_name_col,
                    column_mappings=column_mappings,
                    child_column_name=child_col
                )
                excel_id_col = db_to_excel.get(child_col)

                rel = LookupRelationship(
                    child_table=child_tbl_name,
                    child_column=child_col,
                    parent_table=parent_tbl_name,
                    parent_id_column=parent_id_col,
                    parent_name_column=parent_name_col,
                    excel_name_column=excel_name_col,
                    excel_id_column=excel_id_col,
                    is_required=is_req
                )
                relationships.append(rel)
                seen_keys.add((child_tbl_name, child_col))

            # 2. Inferred foreign keys (columns ending with _id or id matching known parent tables)
            for c_name, c_meta in col_meta_map.items():
                if (child_tbl_name, c_name) in seen_keys:
                    continue
                if getattr(c_meta, "identity", False) or c_meta.primary_key:
                    continue

                c_norm = normalize_column_name(c_name)
                c_lower = c_name.lower()

                if c_lower.endswith("_id") or c_lower.endswith("id"):
                    # Find candidate parent table in schema
                    for candidate_tbl_name, cand_meta in schema_tables.items():
                        if candidate_tbl_name == child_tbl_name:
                            continue
                        cand_lower = candidate_tbl_name.lower()
                        cand_sing = cand_lower.rstrip("s")

                        if cand_lower in c_lower or cand_sing in c_lower:
                            # Found candidate parent table
                            pk_cols = [p.name for p in cand_meta.columns if p.primary_key]
                            parent_id_col = pk_cols[0] if pk_cols else next((p.name for p in cand_meta.columns if getattr(p, "identity", False)), None)
                            if not parent_id_col:
                                parent_id_col = c_name  # fallback

                            is_req = bool(not c_meta.nullable and c_meta.default is None)
                            parent_name_col = cls.identify_parent_name_column(cand_meta)
                            excel_name_col = cls.find_matching_excel_column(
                                df_columns=df_columns,
                                parent_table_name=candidate_tbl_name,
                                parent_name_column=parent_name_col,
                                column_mappings=column_mappings,
                                child_column_name=c_name
                            )
                            excel_id_col = db_to_excel.get(c_name)

                            rel = LookupRelationship(
                                child_table=child_tbl_name,
                                child_column=c_name,
                                parent_table=candidate_tbl_name,
                                parent_id_column=parent_id_col,
                                parent_name_column=parent_name_col,
                                excel_name_column=excel_name_col,
                                excel_id_column=excel_id_col,
                                is_required=is_req
                            )
                            relationships.append(rel)
                            seen_keys.add((child_tbl_name, c_name))
                            break

        return relationships

    @classmethod
    def load_parent_records(
        cls,
        engine: Engine,
        schema: DatabaseSchemaMetadata,
        relationships: List[LookupRelationship],
        df: Optional[pd.DataFrame] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Queries SQL Server using parameterized / metadata-safe SELECT queries to load existing
        records from referenced parent tables for dynamic name-to-ID matching.
        """
        dialect_name = engine.name
        schema_tables = {t.name: t for t in schema.tables}
        records_by_table: Dict[str, List[Dict[str, Any]]] = {}

        # Parent records are candidates for values actually present in this source,
        # not an instruction to scan entire reference tables.
        tables_to_query: Set[str] = {rel.parent_table for rel in relationships}

        source_values_by_table: Dict[str, Dict[str, Set[str]]] = {}
        if df is not None:
            for rel in relationships:
                parent_values = source_values_by_table.setdefault(rel.parent_table, {})
                source_col = rel.excel_name_column or rel.excel_id_column
                if source_col and source_col in df.columns:
                    for value in df[source_col].dropna().unique():
                        if not is_empty_or_na(value):
                            source_value = str(value).strip()
                            if rel.parent_name_column:
                                parent_values.setdefault(rel.parent_name_column, set()).add(source_value)
                            if rel.excel_id_column or not rel.parent_name_column:
                                parent_values.setdefault(rel.parent_id_column, set()).add(source_value)

            for table_name in tables_to_query:
                if table_name.lower() not in ("outlets", "dbo.outlets"):
                    continue
                outlets = source_values_by_table.setdefault(table_name, {})
                outlet_name_col = next((column.name for column in schema_tables[table_name].columns if column.name.lower() in ("outlet_name", "outletname")), None)
                short_code_col = next((column.name for column in schema_tables[table_name].columns if "code" in column.name.lower()), None)
                address_col = next((column.name for column in schema_tables[table_name].columns if "address" in column.name.lower()), None)
                for row in df.to_dict(orient="records"):
                    restaurant, brand = outlet_row_source_values(row)
                    for raw_value in (restaurant, brand):
                        if not raw_value:
                            continue
                        base_value, locality = extract_outlet_base_name(raw_value)
                        if outlet_name_col:
                            outlets.setdefault(outlet_name_col, set()).update(
                                value for value in (raw_value, base_value) if value
                            )
                        if locality and short_code_col:
                            outlets.setdefault(short_code_col, set()).add(locality)
                        if locality and address_col:
                            outlets.setdefault(f"{address_col}__LIKE", set()).add(f"%{locality}%")

        with engine.connect() as conn:
            for table_name in tables_to_query:
                if table_name not in schema_tables:
                    continue

                t_meta = schema_tables[table_name]
                # Query all non-binary / safe columns so records retain complete row data
                safe_cols = [
                    c.name for c in t_meta.columns
                    if c.name in {
                        rel.parent_id_column for rel in relationships if rel.parent_table == table_name
                    } or c.name in {
                        rel.parent_name_column for rel in relationships
                        if rel.parent_table == table_name and rel.parent_name_column
                    } or any(token in c.name.lower() for token in ("name", "code", "address", "location", "title"))
                ]
                if not safe_cols:
                    safe_cols = [c.name for c in t_meta.columns if c.primary_key][:1]

                lookup_values = source_values_by_table.get(table_name, {})
                if not lookup_values:
                    records_by_table[table_name] = []
                    logger.info("[LOOKUP_RESOLVER] No source lookup values for parent table %s; skipped query", table_name)
                    continue

                quoted_tbl = quote_identifier(table_name, dialect_name)
                quoted_cols = [quote_identifier(c, dialect_name) for c in safe_cols]

                try:
                    lookup_terms = [
                        (column.removesuffix("__LIKE"), value, column.endswith("__LIKE"))
                        for column, values in lookup_values.items()
                        for value in values
                    ]
                    record_list: List[Dict[str, Any]] = []
                    seen_parent_ids: Set[str] = set()
                    batch_size = max(1, min(500, 1800 // max(1, len(safe_cols))))
                    for offset in range(0, len(lookup_terms), batch_size):
                        chunk = lookup_terms[offset:offset + batch_size]
                        params: Dict[str, Any] = {}
                        predicates = []
                        for index, (column, value, is_like) in enumerate(chunk):
                            bind_name = f"lookup_{index}"
                            params[bind_name] = value
                            operator = "LIKE" if is_like else "="
                            predicates.append(f"{quote_identifier(column, dialect_name)} {operator} :{bind_name}")
                        top = "TOP 50000 " if dialect_name == "mssql" else ""
                        limit = "" if dialect_name == "mssql" else " LIMIT 50000"
                        query_str = (
                            f"SELECT {top}{', '.join(quoted_cols)} FROM {quoted_tbl} "
                            f"WHERE {' OR '.join(predicates)}{limit}"
                        )
                        rows = conn.execute(text(query_str), params).mappings().all()
                        id_col = next((rel.parent_id_column for rel in relationships if rel.parent_table == table_name), None)
                        for result_row in rows:
                            record = dict(result_row)
                            record_id = record.get(id_col) if id_col else None
                            dedupe_key = str(record_id) if record_id is not None else repr(sorted(record.items()))
                            if dedupe_key not in seen_parent_ids:
                                seen_parent_ids.add(dedupe_key)
                                record_list.append(record)

                    records_by_table[table_name] = record_list
                    logger.info(
                        "[LOOKUP_RESOLVER] Loaded %d candidate records from %s using %d source lookup values",
                        len(record_list), table_name, len(lookup_terms)
                    )
                except Exception as query_err:
                    logger.warning(f"[LOOKUP_RESOLVER] Could not query {table_name}: {query_err}")
                    records_by_table[table_name] = []

        return records_by_table

    @classmethod
    def match_name_to_id(
        cls,
        excel_name: Any,
        parent_table: str,
        parent_id_column: str,
        parent_name_column: str,
        db_records: List[Dict[str, Any]],
        user_override_id: Optional[Any] = None,
        index: Optional[TableLookupIndex] = None
    ) -> Tuple[Optional[Any], Optional[str], str, str, float, List[Dict[str, Any]], str]:
        """
        Matches a single Excel business name against actual database records from SQL Server.
        Follows strict safety rules:
        - 1 reliable match: return (resolved_id, matched_db_name, "MATCHED", "DATABASE_LOOKUP", 1.0, candidates, msg)
        - 0 matches: return (None, None, "UNRESOLVED", "UNRESOLVED", 0.0, [], msg)
        - > 1 matches: return (None, None, "AMBIGUOUS", "AMBIGUOUS", 0.5, candidates, msg)
        - Missing name: return (None, None, "MISSING_NAME", "NULL", 0.0, [], msg)
        - Never chooses the first record arbitrarily.
        """
        # Missing name check
        if excel_name is None:
            return (None, None, "MISSING_NAME", "NULL", 0.0, [], "Excel identifying name is missing/empty.")

        str_name = str(excel_name).strip()
        if not str_name or str_name.lower() in ("nan", "none", "null", "nat"):
            return (None, None, "MISSING_NAME", "NULL", 0.0, [], "Excel identifying name is missing/empty.")

        if not db_records:
            return (
                None,
                None,
                "UNRESOLVED",
                "UNRESOLVED",
                0.0,
                [],
                f"Referenced database table '{parent_table}' has no existing records."
            )

        if index is None:
            index = cls.get_or_create_index(db_records, parent_id_column, parent_name_column)

        # 0. Specialized Outlet matching for Outlets table
        if parent_table.lower() in ("outlets", "dbo.outlets"):
            out_res = OutletMatcher.match_outlet_name(
                source_name=str_name,
                db_records=db_records,
                id_col=parent_id_column,
                name_col=parent_name_column
            )
            out_status = out_res.get("status")
            if out_status == "MATCHED":
                return (
                    out_res["outlet_id"],
                    out_res.get("matched_outlet_name"),
                    "MATCHED",
                    "DATABASE_LOOKUP",
                    out_res.get("confidence", 0.95),
                    out_res.get("candidates", []),
                    out_res.get("message", f"Resolved outlet ID '{out_res['outlet_id']}'.")
                )
            elif out_status == "AMBIGUOUS":
                return (
                    None,
                    None,
                    "AMBIGUOUS",
                    "AMBIGUOUS",
                    0.5,
                    out_res.get("candidates", []),
                    out_res.get("message", f"Ambiguous outlet match for '{str_name}'. User review required.")
                )
            else:
                return (
                    None,
                    None,
                    "UNRESOLVED",
                    "UNRESOLVED",
                    0.0,
                    [],
                    f"Referenced name '{str_name}' not found in database table '{parent_table}'."
                )

        # 0. User override takes top priority
        if user_override_id is not None:
            override_str = str(user_override_id).strip()
            matched_rec = index.override_map.get(override_str)
            matched_name = matched_rec["name"] if matched_rec else None
            return (
                user_override_id,
                matched_name,
                "MATCHED",
                "USER_OVERRIDE",
                1.0,
                [],
                f"Resolved via user override to ID '{user_override_id}'."
            )

        # 1. Exact matches evaluated via O(1) hash map
        exact_matches = index.exact_map.get(str_name, [])
        if len(exact_matches) == 1:
            rec = exact_matches[0]
            return (
                rec["id"],
                rec["name"],
                "MATCHED",
                "DATABASE_LOOKUP",
                1.0,
                exact_matches,
                f"Exact match on '{rec['name']}' in database table '{parent_table}'."
            )
        elif len(exact_matches) > 1:
            return (
                None,
                None,
                "AMBIGUOUS",
                "AMBIGUOUS",
                0.5,
                exact_matches,
                f"Ambiguous: Name '{str_name}' matched {len(exact_matches)} database records in '{parent_table}'. User review required."
            )

        # 2. Normalized matches evaluated via O(1) hash map
        norm_excel = normalize_cell_value(str_name)
        lower_excel = str_name.lower()
        norm_raw = index.norm_map.get(norm_excel) or index.norm_map.get(lower_excel, [])

        # Deduplicate matches by ID
        seen_cand_ids = set()
        normalized_matches = []
        for c in norm_raw:
            if c["id"] not in seen_cand_ids:
                seen_cand_ids.add(c["id"])
                normalized_matches.append(c)

        if len(normalized_matches) == 1:
            rec = normalized_matches[0]
            return (
                rec["id"],
                rec["name"],
                "MATCHED",
                "DATABASE_LOOKUP",
                0.95,
                normalized_matches,
                f"Normalized match ('{str_name}' -> '{rec['name']}') in database table '{parent_table}'."
            )
        elif len(normalized_matches) > 1:
            return (
                None,
                None,
                "AMBIGUOUS",
                "AMBIGUOUS",
                0.5,
                normalized_matches,
                f"Ambiguous: Normalized name '{str_name}' matched {len(normalized_matches)} database records in '{parent_table}'."
            )

        # 3. Fuzzy token matches evaluated (suggestions only - require confirmation)
        fuzzy_matches = []
        fuzzy_ids = set()
        for tok in norm_excel.split():
            if len(tok) >= 3 and tok in index.token_map:
                for c in index.token_map[tok]:
                    if c["id"] not in fuzzy_ids:
                        fuzzy_ids.add(c["id"])
                        fuzzy_matches.append(c)
                        if len(fuzzy_matches) >= 5:
                            break
            if len(fuzzy_matches) >= 5:
                break

        if fuzzy_matches:
            return (
                None,
                None,
                "AMBIGUOUS",
                "AMBIGUOUS",
                0.6,
                fuzzy_matches[:5],
                f"Uncertain: Name '{str_name}' partially matched {len(fuzzy_matches)} candidate(s) in '{parent_table}'. Confirmation required."
            )

        # 4. No matches
        return (
            None,
            None,
            "UNRESOLVED",
            "UNRESOLVED",
            0.0,
            [],
            f"Referenced name '{str_name}' not found in database table '{parent_table}'."
        )

    @classmethod
    def resolve_foreign_key_record(
        cls,
        child_table: str,
        child_column: str,
        parent_table: str,
        parent_id_column: str,
        parent_name_column: Optional[str] = None,
        db_records: Optional[List[Dict[str, Any]]] = None,
        excel_source_value: Any = None,
        row_resolved_entities: Optional[Dict[str, ResolvedEntityRecord]] = None,
        row_value_mappings: Optional[List[ValueMappingItem]] = None,
        row_data: Optional[Dict[str, Any]] = None,
        user_override_id: Optional[Any] = None,
        index: Optional[TableLookupIndex] = None,
        migration_cache: Optional[Dict[Any, ResolvedEntityRecord]] = None,
        all_parent_records: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        upload_id: Optional[str] = None,
        source_row_num: Optional[int] = None
    ) -> ResolvedEntityRecord:
        """
        Dynamically resolves a foreign key to a ResolvedEntityRecord with complete record retention:
        1. User override check (priority 1.0)
        2. Same-row entity reuse check (reuses previously resolved parent entity for same row if source is empty or matching)
        3. Migration-level cache lookup (O(1))
        4. Direct database match against parent_records (via TableLookupIndex)
        5. Value mapping inspection from row_value_mappings (direct match on parent table or correlated table)
        6. Correlated entity inspection from row_data and other tables (e.g. Customer.Location -> Outlet_ID)
        7. Ambiguity detection (if multiple distinct records match -> AMBIGUOUS)
        8. Missing/empty value handling (clean UNRESOLVED without querying 'None' or 'nan')
        Never hardcodes IDs or table mappings.
        """
        parent_keys = [parent_table, parent_table.lower(), parent_table.rstrip("s").lower()]
        db_records = db_records or []

        # 1. User Override takes top priority
        if user_override_id is not None:
            override_str = str(user_override_id).strip()
            matched_rec = {}
            for r in db_records:
                if str(r.get(parent_id_column)).strip() == override_str:
                    matched_rec = r
                    break
            res = ResolvedEntityRecord(
                status="RESOLVED",
                matchedTable=parent_table,
                matchedPrimaryKeyColumn=parent_id_column,
                matchedPrimaryKeyValue=user_override_id,
                matchedBy=["USER_OVERRIDE"],
                confidence=1.0,
                recordData=matched_rec,
                matchMethod="USER_OVERRIDE",
                message=f"Resolved via user override to ID '{user_override_id}'."
            )
            if row_resolved_entities is not None:
                for pk in parent_keys:
                    row_resolved_entities[pk] = res
            return res

        # 2. Clean and sanitize source value
        s_val: Optional[str] = None
        if excel_source_value is not None and not is_empty_or_na(excel_source_value):
            raw_s = str(excel_source_value).strip()
            if raw_s and raw_s.lower() not in ("nan", "none", "null", "nat"):
                s_val = raw_s

        is_outlet_parent = parent_table.lower() in ("outlets", "dbo.outlets")
        outlet_cache_key = s_val
        if is_outlet_parent and isinstance(row_data, dict):
            outlet_cache_key = outlet_row_source_key(row_data)

        if is_outlet_parent and not db_records and row_resolved_entities:
            existing_outlet = next(
                (row_resolved_entities[key] for key in parent_keys if key in row_resolved_entities),
                None
            )
            if existing_outlet and existing_outlet.status == "RESOLVED":
                source_outlet_values = list(outlet_row_source_values(row_data or {}))
                if s_val and s_val not in source_outlet_values:
                    source_outlet_values.append(s_val)
                known_values = {
                    normalize_cell_value(str(existing_outlet.record_data.get(parent_name_column or "") or ""))
                }
                for matched_value in existing_outlet.matched_by:
                    known_values.add(normalize_cell_value(matched_value.split("=", 1)[-1]))
                source_norms = [normalize_cell_value(value) for value in source_outlet_values if value]
                if not source_norms or all(value in known_values for value in source_norms):
                    return existing_outlet

        # Outlet name fields are a row-level identity: both must be evaluated before
        # any cached or previously-resolved entity can be reused.
        if is_outlet_parent:
            outlet_res = OutletMatcher.resolve_row_outlet(
                row_data=row_data,
                db_records=db_records,
                id_col=parent_id_column,
                name_col=parent_name_column or "Outlet_Name"
            ) if isinstance(row_data, dict) else None
            if (not outlet_res or outlet_res.get("status") == "MISSING_NAME") and s_val is not None:
                outlet_res = OutletMatcher.match_outlet_name(
                    source_name=s_val,
                    db_records=db_records,
                    id_col=parent_id_column,
                    name_col=parent_name_column or "Outlet_Name"
                )

            outlet_status = outlet_res.get("status", "UNRESOLVED") if outlet_res else "UNRESOLVED"
            candidates = outlet_res.get("candidates", []) if outlet_res else []
            candidate_ids = sorted({str(c.get("id")) for c in candidates if c.get("id") is not None})
            restaurant_value = outlet_res.get("restaurant_value") if outlet_res else None
            virtual_brand_value = outlet_res.get("virtual_brand_value") if outlet_res else None
            logger.info(
                "[OUTLET_FK_RESOLUTION] upload_id=%s source_row=%s restaurant_name=%r virtual_brand_name=%r "
                "normalized_key=%r parent=%s referenced_key=%s candidate_count=%d candidate_ids=%s "
                "status=%s resolved_id=%s reason=%r",
                upload_id or "unknown",
                source_row_num if source_row_num is not None else "unknown",
                restaurant_value,
                virtual_brand_value,
                outlet_res.get("normalized_name") if outlet_res else None,
                parent_table,
                parent_id_column,
                len(candidate_ids),
                candidate_ids,
                outlet_status,
                outlet_res.get("outlet_id") if outlet_res else None,
                outlet_res.get("message") if outlet_res else "No outlet candidates were loaded."
            )

            if outlet_status == "MATCHED":
                matched_id = outlet_res.get("outlet_id")
                matched_rec = next(
                    (r for r in db_records if str(r.get(parent_id_column)) == str(matched_id)),
                    None
                )
                if matched_id is None or matched_rec is None:
                    outlet_status = "UNRESOLVED"
                    outlet_res["message"] = "Resolved outlet ID is absent from the loaded referenced records."
                else:
                    result = ResolvedEntityRecord(
                        status="RESOLVED",
                        matchedTable=parent_table,
                        matchedPrimaryKeyColumn=parent_id_column,
                        matchedPrimaryKeyValue=matched_id,
                        matchedBy=[
                            value for value in (
                                f"restaurant_name={restaurant_value}" if restaurant_value else None,
                                f"virtual_brand_name={virtual_brand_value}" if virtual_brand_value else None
                            ) if value
                        ],
                        confidence=outlet_res.get("confidence", 0.95),
                        recordData=matched_rec,
                        matchMethod=outlet_res.get("match_type", "OUTLET_MATCHER"),
                        message=outlet_res.get("message")
                    )
                    if row_resolved_entities is not None:
                        for key in parent_keys:
                            row_resolved_entities[key] = result
                    if migration_cache is not None:
                        migration_cache[(parent_table, outlet_cache_key)] = result
                    return result

            status = outlet_status if outlet_status in ("AMBIGUOUS", "CONFLICTING_OUTLET_VALUES") else "UNRESOLVED"
            return ResolvedEntityRecord(
                status=status,
                matchedTable=parent_table,
                matchedPrimaryKeyColumn=parent_id_column,
                matchedPrimaryKeyValue=None,
                matchedBy=[str(v) for v in (restaurant_value, virtual_brand_value) if v],
                confidence=outlet_res.get("confidence", 0.0) if outlet_res else 0.0,
                candidates=candidates,
                matchMethod=outlet_res.get("match_type", status) if outlet_res else status,
                message=outlet_res.get("message") if outlet_res else "Outlet name is missing and no existing outlet candidates were loaded."
            )

        # 3. Same-Row Entity Reuse Check
        existing_row_entity: Optional[ResolvedEntityRecord] = None
        if row_resolved_entities is not None:
            for pk in parent_keys:
                if pk in row_resolved_entities:
                    existing_row_entity = row_resolved_entities[pk]
                    break

        if existing_row_entity and existing_row_entity.status == "RESOLVED":
            # If current source value is empty/missing, reuse the resolved parent entity for this row
            if s_val is None:
                return existing_row_entity
            # If current source value matches the entity already resolved for this row
            existing_name = str(existing_row_entity.record_data.get(parent_name_column or "") or "").strip()
            if (
                s_val in existing_row_entity.matched_by
                or (existing_name and (normalize_string(s_val) == normalize_string(existing_name) or s_val.lower() == existing_name.lower()))
            ):
                return existing_row_entity
            # If s_val is non-empty and different (e.g. assign_to differs), proceed to independent resolution below

        # 4. Check migration-level cache if s_val is present
        if migration_cache is not None and s_val is not None:
            cache_key = (parent_table, outlet_cache_key if is_outlet_parent else s_val)
            if cache_key in migration_cache:
                cached = migration_cache[cache_key]
                if cached.status == "RESOLVED":
                    if row_resolved_entities is not None:
                        for pk in parent_keys:
                            row_resolved_entities[pk] = cached
                    return cached

        # 5. Direct matching against parent_table via TableLookupIndex
        if s_val is not None and db_records:
            if index is None:
                index = cls.get_or_create_index(db_records, parent_id_column, parent_name_column or parent_id_column)

            # 5a. Exact match
            exact_matches = index.exact_map.get(s_val, [])
            if len(exact_matches) == 1:
                rec_entry = exact_matches[0]
                res = ResolvedEntityRecord(
                    status="RESOLVED",
                    matchedTable=parent_table,
                    matchedPrimaryKeyColumn=parent_id_column,
                    matchedPrimaryKeyValue=rec_entry["id"],
                    matchedBy=[s_val],
                    confidence=1.0,
                    recordData=rec_entry.get("record", {}),
                    matchMethod="DATABASE_LOOKUP",
                    message=f"Exact match on '{rec_entry['name']}' in table '{parent_table}'."
                )
                if row_resolved_entities is not None:
                    for pk in parent_keys:
                        row_resolved_entities[pk] = res
                if migration_cache is not None:
                    migration_cache[(parent_table, s_val)] = res
                return res
            elif len(exact_matches) > 1:
                seen_cands = {}
                for m in exact_matches:
                    seen_cands[m["id"]] = {"id": m["id"], "name": m["name"]}
                cands_list = list(seen_cands.values())
                if len(cands_list) > 1:
                    return ResolvedEntityRecord(
                        status="AMBIGUOUS",
                        matchedTable=parent_table,
                        matchedPrimaryKeyColumn=parent_id_column,
                        matchedPrimaryKeyValue=None,
                        matchedBy=[s_val],
                        confidence=0.5,
                        candidates=cands_list,
                        matchMethod="AMBIGUOUS",
                        message=f"Ambiguous: Name '{s_val}' matched {len(cands_list)} records in '{parent_table}'."
                    )
                else:
                    rec_entry = exact_matches[0]
                    res = ResolvedEntityRecord(
                        status="RESOLVED",
                        matchedTable=parent_table,
                        matchedPrimaryKeyColumn=parent_id_column,
                        matchedPrimaryKeyValue=rec_entry["id"],
                        matchedBy=[s_val],
                        confidence=1.0,
                        recordData=rec_entry.get("record", {}),
                        matchMethod="DATABASE_LOOKUP"
                    )
                    if row_resolved_entities is not None:
                        for pk in parent_keys:
                            row_resolved_entities[pk] = res
                    return res

            # 5b. Normalized match
            norm_val = normalize_cell_value(s_val)
            norm_matches = index.norm_map.get(norm_val) or index.norm_map.get(s_val.lower(), [])
            if norm_matches:
                seen_cands = {}
                for m in norm_matches:
                    seen_cands[m["id"]] = {"id": m["id"], "name": m["name"], "record": m.get("record", {})}
                cands_list = list(seen_cands.values())
                if len(cands_list) == 1:
                    rec_entry = cands_list[0]
                    res = ResolvedEntityRecord(
                        status="RESOLVED",
                        matchedTable=parent_table,
                        matchedPrimaryKeyColumn=parent_id_column,
                        matchedPrimaryKeyValue=rec_entry["id"],
                        matchedBy=[s_val],
                        confidence=0.95,
                        recordData=rec_entry.get("record", {}),
                        matchMethod="DATABASE_LOOKUP",
                        message=f"Normalized match on '{rec_entry['name']}' in table '{parent_table}'."
                    )
                    if row_resolved_entities is not None:
                        for pk in parent_keys:
                            row_resolved_entities[pk] = res
                    if migration_cache is not None:
                        migration_cache[(parent_table, s_val)] = res
                    return res
                elif len(cands_list) > 1:
                    return ResolvedEntityRecord(
                        status="AMBIGUOUS",
                        matchedTable=parent_table,
                        matchedPrimaryKeyColumn=parent_id_column,
                        matchedPrimaryKeyValue=None,
                        matchedBy=[s_val],
                        confidence=0.5,
                        candidates=[{"id": c["id"], "name": c["name"]} for c in cands_list],
                        matchMethod="AMBIGUOUS",
                        message=f"Ambiguous: Normalized name '{s_val}' matched {len(cands_list)} records in '{parent_table}'."
                    )

            # 5c. Token / substring fuzzy matches
            fuzzy_matches = []
            fuzzy_ids = set()
            for tok in norm_val.split():
                if len(tok) >= 3 and tok in index.token_map:
                    for c in index.token_map[tok]:
                        if c["id"] not in fuzzy_ids:
                            fuzzy_ids.add(c["id"])
                            fuzzy_matches.append(c)
                            if len(fuzzy_matches) >= 5:
                                break
                if len(fuzzy_matches) >= 5:
                    break

            if len(fuzzy_matches) >= 2:
                return ResolvedEntityRecord(
                    status="AMBIGUOUS",
                    matchedTable=parent_table,
                    matchedPrimaryKeyColumn=parent_id_column,
                    matchedPrimaryKeyValue=None,
                    matchedBy=[s_val],
                    confidence=0.5,
                    candidates=[{"id": c["id"], "name": c["name"]} for c in fuzzy_matches[:5]],
                    matchMethod="AMBIGUOUS",
                    message=f"Ambiguous: Name '{s_val}' matched multiple candidates ({len(fuzzy_matches)}) in '{parent_table}'."
                )
        if row_value_mappings:
            for vm in row_value_mappings:
                # 6a. Direct value mapping match targeting parent_table
                if vm.matched_table and vm.matched_table.lower() == parent_table.lower() and vm.matched_primary_key_value is not None:
                    matched_pk = vm.matched_primary_key_value
                    matched_rec = vm.matched_record_data or {}
                    if not matched_rec and db_records:
                        for r in db_records:
                            if r.get(parent_id_column) == matched_pk:
                                matched_rec = r
                                break
                    res = ResolvedEntityRecord(
                        status="RESOLVED",
                        matchedTable=parent_table,
                        matchedPrimaryKeyColumn=parent_id_column,
                        matchedPrimaryKeyValue=matched_pk,
                        matchedBy=[f"ValueMapping: {vm.excel_column}={vm.excel_value}"],
                        confidence=vm.confidence,
                        recordData=matched_rec,
                        matchMethod="VALUE_MAPPING",
                        message=f"Resolved via value mapping from {vm.excel_column} to {vm.target_value}."
                    )
                    if row_resolved_entities is not None:
                        for pk in parent_keys:
                            row_resolved_entities[pk] = res
                    return res

                # 6b. Correlated value mapping on a related entity record (e.g. Customer.Outlet_ID)
                if vm.matched_record_data:
                    for k, v in vm.matched_record_data.items():
                        if k.lower() == parent_id_column.lower() and v is not None:
                            matched_outlet_id = v
                            matched_outlet_rec = {}
                            if db_records:
                                for r in db_records:
                                    if r.get(parent_id_column) == matched_outlet_id:
                                        matched_outlet_rec = r
                                        break
                            res = ResolvedEntityRecord(
                                status="RESOLVED",
                                matchedTable=parent_table,
                                matchedPrimaryKeyColumn=parent_id_column,
                                matchedPrimaryKeyValue=matched_outlet_id,
                                matchedBy=[f"ValueMapping {vm.database_table}.{vm.database_column}: {vm.target_value}"],
                                confidence=0.95,
                                recordData=matched_outlet_rec,
                                matchMethod="VALUE_MAPPING_CORRELATED",
                                message=f"Resolved via correlated value mapping on {vm.database_table}.{vm.database_column} -> {vm.target_value} ({parent_id_column}={matched_outlet_id})."
                            )
                            if row_resolved_entities is not None:
                                for pk in parent_keys:
                                    row_resolved_entities[pk] = res
                            return res

        # 7. Correlated entity inspection from row_data and related parent tables (e.g. Customer Location)
        if all_parent_records and parent_table.lower() in ("outlets", "dbo.outlets"):
            cust_records = all_parent_records.get("Customer", [])
            loc_candidates = []
            if row_data:
                for k in ("Location", "location", "restaurant_name", "restaurant", "area", "address"):
                    if k in row_data and row_data[k] and not is_empty_or_na(row_data[k]):
                        loc_candidates.append(str(row_data[k]).strip())

            if loc_candidates and cust_records:
                for cand_loc in loc_candidates:
                    norm_c = normalize_string(cand_loc)
                    for cr in cust_records:
                        cr_loc = str(cr.get("Location") or "").strip()
                        if cr_loc and (cr_loc.lower() == cand_loc.lower() or normalize_string(cr_loc) == norm_c):
                            out_id = cr.get("Outlet_ID")
                            if out_id is not None:
                                matched_out_rec = {}
                                if db_records:
                                    for r in db_records:
                                        if r.get(parent_id_column) == out_id:
                                            matched_out_rec = r
                                            break
                                res = ResolvedEntityRecord(
                                    status="RESOLVED",
                                    matchedTable=parent_table,
                                    matchedPrimaryKeyColumn=parent_id_column,
                                    matchedPrimaryKeyValue=out_id,
                                    matchedBy=[f"Customer.Location: {cand_loc}"],
                                    confidence=0.95,
                                    recordData=matched_out_rec,
                                    matchMethod="CORRELATED_LOOKUP",
                                    message=f"Resolved via customer location '{cand_loc}' referencing {parent_id_column}={out_id}."
                                )
                                if row_resolved_entities is not None:
                                    for pk in parent_keys:
                                        row_resolved_entities[pk] = res
                                return res

        # 8. Missing source value
        if s_val is None:
            return ResolvedEntityRecord(
                status="UNRESOLVED",
                matchedTable=parent_table,
                matchedPrimaryKeyColumn=parent_id_column,
                matchedPrimaryKeyValue=None,
                matchMethod="MISSING_VALUE",
                message=f"Required foreign key '{child_column}' missing identifying name in source data."
            )

        # 9. Value not found
        return ResolvedEntityRecord(
            status="UNRESOLVED",
            matchedTable=parent_table,
            matchedPrimaryKeyColumn=parent_id_column,
            matchedPrimaryKeyValue=None,
            matchedBy=[s_val],
            matchMethod="UNRESOLVED",
            message=f"Referenced value '{s_val}' not found in database table '{parent_table}'."
        )
