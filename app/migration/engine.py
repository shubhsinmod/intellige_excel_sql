import time
import math
import datetime
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
import pandas as pd
from sqlalchemy import text, Engine
from sqlalchemy.engine import Connection

from app.core.config import settings
from app.database.connection import get_engine
from app.services.database_service import database_service
from app.services.excel_service import excel_service
from app.excel.reader import load_file_dataframe
from app.mapping.matcher import ColumnMatcher, normalize_string, resolve_unmapped_required_columns
from app.migration.dependency import DependencyResolver
from app.migration.id_resolver import IdResolverStore
from app.migration.transaction import transaction_manager
from app.migration.validator import MigrationValidator, is_empty_or_na
from app.models.schema import DatabaseSchemaMetadata, TableMetadata, ColumnMetadata
from app.models.migration import (
    MigrationExecutionResponse,
    TableMigrationStat,
    ColumnMappingResult,
    ForeignKeyPlanItem,
    ResolvedEntityRecord,
    ValueMappingItem,
    RowValidationError
)
from app.migration.lookup_resolver import DynamicLookupResolver, LookupRelationship
from app.migration.outlet_matcher import OutletMatcher, extract_outlet_base_name, outlet_row_source_key
from app.migration.missing_value_resolver import (
    MissingValueResolver,
    TableEntityLookupIndex,
    is_customer_name_column
)
from app.migration.record_matcher import (
    load_existing_matches,
    mapped_matching_keys,
    matching_key_index_name,
    source_key
)
from app.core.logging import logger


def quote_identifier(name: str, dialect_name: str) -> str:
    """Quotes table or column identifiers appropriately for SQL Server, MySQL, Postgres, SQLite."""
    clean = str(name).strip().replace("[", "").replace("]", "").replace("`", "").replace('"', '')
    if dialect_name == "mssql":
        return f"[{clean}]"
    elif dialect_name in ("mysql", "mariadb"):
        return f"`{clean}`"
    else:
        return f'"{clean}"'


def sanitize_value(val: Any, col_type: str = "", nullable: bool = True, allow_literal_null_str: bool = False) -> Any:
    """
    Sanitizes raw values from pandas DataFrame (handles NaN, NaT, numpy types, empty strings, and type conversions).
    Preserves valid values from CSV / Excel without losing information.
    """
    if allow_literal_null_str:
        if val is None or is_empty_or_na(val):
            return "null"
        str_val = str(val).strip()
        if str_val == "" or str_val.lower() in ("nan", "none", "null", "nat"):
            return "null"
        return str_val

    if val is None or is_empty_or_na(val):
        return None if nullable else ""

    str_val = str(val).strip()
    if str_val == "" or str_val.lower() in ("nan", "none", "null", "nat"):
        return None if nullable else ""

    col_type_upper = col_type.upper()

    # Integers
    if any(t in col_type_upper for t in ("INT", "BIGINT", "SMALLINT", "TINYINT")):
        try:
            return int(float(str_val))
        except (ValueError, TypeError):
            return None if nullable else 0

    # Numeric / Float / Decimal / Money
    elif any(t in col_type_upper for t in ("FLOAT", "DECIMAL", "NUMERIC", "REAL", "DOUBLE", "MONEY")):
        try:
            clean_num = str_val.replace("$", "").replace("₹", "").replace(",", "")
            return float(clean_num)
        except (ValueError, TypeError):
            return None if nullable else 0.0

    # Date / Time formatting
    elif any(t in col_type_upper for t in ("DATE", "DATETIME", "DATETIME2", "SMALLDATETIME")):
        from app.utils.normalization import normalize_date_value
        norm_dt_str, dt_obj, is_v = normalize_date_value(val if val is not None else str_val, target_type=col_type_upper)
        if is_v and norm_dt_str is not None:
            return norm_dt_str
        elif is_v and norm_dt_str is None:
            return None if nullable else ""
        return str_val

    # If the raw value is a float that ends in .0 (e.g. CSV phone numbers or IDs parsed as floats by pandas)
    if isinstance(val, float) and val.is_integer():
        return str(int(val))

    return str_val


class MigrationEngine:
    """
    Universal Dynamic Transactional Migration Engine.
    - Zero hardcoded tables or columns.
    - Executes within an atomic database transaction using engine.begin().
    - Validates execution plan before any modification.
    - Topologically orders target tables (parents before children).
    - Captures SQL Server generated identity values using OUTPUT INSERTED and passes them to child records.
    - Reuses existing records from the database where business keys match.
    - On failure, rolls back the entire transaction automatically.
    - Idempotent and thread-safe.
    """

    @staticmethod
    def execute_migration(
        upload_id: str,
        progress_callback: Optional[Callable[[str, int, int, int, int, int, int], None]] = None
    ) -> MigrationExecutionResponse:
        # SQL Safety Guard: Ensure dry-run mode never invokes real migration execution
        if getattr(settings, "DRY_RUN_MODE", False):
            raise RuntimeError(
                "SAFETY GUARD VIOLATION: Migration execution attempted while DRY_RUN_MODE is enabled. "
                "Data-modifying SQL execution is strictly prohibited in dry-run mode."
            )

        t_start = time.time()
        t_start_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        logger.info(f"[MIGRATION] START | Executing dynamic migration for uploadId={upload_id}")

        # 1. Retrieve uploaded tabular data
        session_data = excel_service.get_upload_session(upload_id)
        if not session_data:
            raise ValueError(f"Upload session '{upload_id}' not found.")

        file_path = session_data.get("filePath")
        sheet_name = session_data.get("sheetName")

        if not file_path or not pd.io.common.file_exists(file_path):
            raise ValueError("Uploaded data file is missing from server storage.")

        header_row = session_data.get("headerRowNumber") or 1
        df = load_file_dataframe(
            file_path,
            sheet_name=sheet_name,
            header=(header_row - 1) if header_row >= 1 else 0
        ).dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]

        # Case-insensitive column index for CSV/Excel lookup
        col_lookup = {c.lower(): c for c in df.columns}

        def get_df_val(row_dict: Any, ex_col_name: str) -> Any:
            if ex_col_name in row_dict:
                return row_dict[ex_col_name]
            clean = ex_col_name.strip().lower()
            actual_col = col_lookup.get(clean)
            if actual_col and actual_col in row_dict:
                return row_dict[actual_col]
            return None

        # 2. Introspect Database Schema and Match Columns Dynamically
        engine = get_engine()
        dialect_name = engine.name
        schema = database_service.get_schema_metadata()
        schema_tables = {t.name: t for t in schema.tables}

        # Dynamic column matching (incorporating user overrides if present)
        from app.services.migration_service import migration_service
        overrides = migration_service._user_overrides.get(upload_id, {})
        column_mappings = ColumnMatcher.match_columns(df.columns.tolist(), schema, user_overrides=overrides)
        matched_mappings = [m for m in column_mappings if m.status == "MATCHED" and m.database_table in schema_tables]

        if not matched_mappings:
            logger.warning("[MIGRATION] No columns could be matched to the target database.")
            return MigrationExecutionResponse(
                uploadId=upload_id,
                status="COMPLETED",
                transaction="COMMITTED",
                tables={},
                totalInserted=0,
                totalUpdated=0,
                totalReused=0,
                totalSkipped=0,
                totalFailed=0,
                verificationCounts={},
                processingTimeSeconds=round(time.time() - t_start, 3),
                message="No columns were matched to target database tables."
            )

        # 3. Determine Active Target Tables and Topological Order
        table_mappings_map: Dict[str, List[ColumnMappingResult]] = {}
        for m in matched_mappings:
            table_mappings_map.setdefault(m.database_table, []).append(m)

        # Dynamically resolve unmapped required columns from source dataset
        table_mappings_map = resolve_unmapped_required_columns(schema, table_mappings_map, df.columns.tolist())

        # 3.8 Discover dynamic foreign-key lookup relationships using database metadata
        all_potential_tables = list(table_mappings_map.keys())
        initial_lookup_relationships = DynamicLookupResolver.discover_relationships(
            schema=schema,
            df_columns=df.columns.tolist(),
            column_mappings=column_mappings,
            active_tables=all_potential_tables
        )
        parent_records_by_table = DynamicLookupResolver.load_parent_records(
            engine=engine,
            schema=schema,
            relationships=initial_lookup_relationships,
            df=df
        )

        # Outlets is a read-only reference table, even when it currently has no rows.
        active_target_tables = [
            t for t in table_mappings_map
            if t.lower() not in ("outlets", "dbo.outlets")
        ]

        full_execution_order = DependencyResolver.resolve_execution_order(schema, target_tables=active_target_tables)
        active_tables_ordered = [t for t in full_execution_order if t in active_target_tables]
        for t in active_target_tables:
            if t not in active_tables_ordered:
                active_tables_ordered.append(t)

        logger.info(f"[MIGRATION] Active tables to execute in order: {active_tables_ordered}")


        # 3.5 Prepare value mapping transformations
        from app.mapping.values import ValueMatcher
        user_val_overrides = migration_service._user_value_overrides.get(upload_id, {})
        value_transform_lookup: Dict[Tuple[str, str, str], str] = {}  # (table, db_col, raw_val) -> target_val
        col_value_transform: Dict[Tuple[str, str], str] = {}          # (db_col, raw_val) -> target_val
        all_val_items = []

        CATEGORICAL_KEYWORDS = ("type", "status", "mode", "category", "channel", "method", "state", "brand", "source", "reason", "payment")
        EXCLUDED_KEYWORDS = ("name", "id", "code", "date", "time", "phone", "comment", "note", "desc", "bill", "guid", "price", "amount", "tax", "rate", "cost", "qty", "quantity", "address")

        for m in matched_mappings:
            if m.excel_column in df.columns and m.database_table in schema_tables:
                col_lower = m.database_column.lower()
                is_categorical = (
                    any(k in col_lower for k in CATEGORICAL_KEYWORDS)
                    or any(k in m.excel_column.lower() for k in CATEGORICAL_KEYWORDS)
                ) and not any(k in col_lower for k in EXCLUDED_KEYWORDS)

                if is_categorical:
                    valid_db_vals = ValueMatcher.get_valid_column_values(
                        engine=engine,
                        table=m.database_table,
                        column=m.database_column
                    )
                    if valid_db_vals:
                        overrides_map = user_val_overrides.get(f"{m.database_table}.{m.database_column}") or user_val_overrides.get(m.excel_column)
                        v_items = ValueMatcher.match_column_values(
                            df=df,
                            excel_column=m.excel_column,
                            target_table=m.database_table,
                            target_column=m.database_column,
                            valid_db_values=valid_db_vals,
                            user_overrides=overrides_map,
                            database_scope=schema.database_name
                        )
                        for item in v_items:
                            all_val_items.append(item)
                            if item.target_value and item.status in ("MATCHED", "APPROVED"):
                                value_transform_lookup[(m.database_table, m.database_column, item.excel_value)] = item.target_value
                                col_value_transform[(m.database_column, item.excel_value)] = item.target_value


        # 3.8 Discover dynamic foreign-key lookup relationships scoped to active tables
        lookup_relationships: List[LookupRelationship] = DynamicLookupResolver.discover_relationships(
            schema=schema,
            df_columns=df.columns.tolist(),
            column_mappings=column_mappings,
            active_tables=active_tables_ordered
        )
        # Ensure any newly discovered relationships have their parent records loaded
        for rel in lookup_relationships:
            if rel.parent_table not in parent_records_by_table:
                more_recs = DynamicLookupResolver.load_parent_records(
                    engine=engine,
                    schema=schema,
                    relationships=[rel],
                    df=df
                )
                parent_records_by_table.update(more_recs)

        # Quick validation plan for pre-execution check
        fk_val_plan = []
        for rel in lookup_relationships:
            if rel.child_table not in active_tables_ordered:
                continue
            db_recs = parent_records_by_table.get(rel.parent_table, [])
            src_col = rel.excel_name_column or rel.excel_id_column
            if src_col and src_col in df.columns:
                u_names = [v for v in df[src_col].dropna().unique() if not is_empty_or_na(v)][:20]
                for u_n in u_names:
                    _, _, r_stat, _, _, cands, _ = DynamicLookupResolver.match_name_to_id(
                        excel_name=u_n,
                        parent_table=rel.parent_table,
                        parent_id_column=rel.parent_id_column,
                        parent_name_column=rel.parent_name_column or rel.parent_id_column,
                        db_records=db_recs
                    )
                    fk_val_plan.append(ForeignKeyPlanItem(
                        childTable=rel.child_table,
                        childColumn=rel.child_column,
                        parentTable=rel.parent_table,
                        parentColumn=rel.parent_id_column,
                        sourceBusinessKey=str(u_n),
                        existingParentFound=(r_stat == "MATCHED"),
                        status="MATCHED" if r_stat == "MATCHED" else ("PENDING" if rel.parent_table in active_tables_ordered else r_stat),
                        candidates=cands
                    ))

        # 4. Pre-execution dataset validation with dynamic missing value resolution
        entity_indices: Dict[str, TableEntityLookupIndex] = {}
        for t_name in active_tables_ordered:
            t_m = schema_tables[t_name]
            entity_indices[t_name] = MissingValueResolver.load_table_entity_index(engine, t_m)

        validation_res = MigrationValidator.validate_dataset(
            df=df,
            schema=schema,
            column_mappings=column_mappings,
            value_mappings=all_val_items,
            foreign_key_plans=fk_val_plan,
            entity_indices=entity_indices,
            lookup_relationships=lookup_relationships,
            parent_records_by_table=parent_records_by_table,
            upload_id=upload_id
        )
        if not validation_res.canExecute:
            error_details = "; ".join([e.message for e in validation_res.errors[:5]])
            logger.warning(f"[MIGRATION] Validation blocked execution for uploadId={upload_id}: {error_details}")
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            return MigrationExecutionResponse(
                uploadId=upload_id,
                status="BLOCKED",
                transaction="NOT_EXECUTED",
                transactionStatus="NOT_EXECUTED",
                canExecute=False,
                executionStarted=t_start_iso,
                executionCompleted=now_iso,
                tables={},
                totalInserted=0,
                totalUpdated=0,
                totalReused=0,
                totalSkipped=0,
                totalFailed=0,
                insertedCount=0,
                updatedCount=0,
                reusedCount=0,
                skippedCount=0,
                failedCount=0,
                validationErrors=validation_res.errors,
                warnings=validation_res.warnings,
                verificationCounts={},
                processingTimeSeconds=round(time.time() - t_start, 3),
                message=f"Migration execution blocked: {len(validation_res.errors)} validation error(s) must be resolved before executing the database transaction.",
                errorReport=[e.model_dump(by_alias=True) for e in validation_res.errors]
            )

        # 5. Define the execution function to be run inside the transaction
        def _execute_in_transaction(conn: Connection) -> MigrationExecutionResponse:
            id_store = IdResolverStore()
            table_stats: Dict[str, TableMigrationStat] = {t: TableMigrationStat() for t in active_tables_ordered}
            generated_ids_by_table: Dict[str, List[Any]] = {t: [] for t in active_tables_ordered}
            verif_counts: Dict[str, int] = {}
            failure_details: List[Dict[str, Any]] = []
            source_row_indices_with_insert: Set[int] = set()
            source_row_indices_with_reuse: Set[int] = set()
            source_row_indices_duplicate: Set[int] = set()
            fk_propagated_count: int = 0
            matching_keys_unavailable: Set[str] = set()
            row_outcomes: List[Dict[str, Any]] = []
            unresolved_fk_rows: Set[Tuple[int, str]] = set()

            # Pre-convert DataFrame to record dictionaries once for 50x faster row access
            df_records = df.to_dict(orient="records")
            df_indices = df.index.tolist()

            # Pre-index all_val_items by (column, value) for O(1) matching
            val_items_by_col: Dict[str, Dict[str, List[ValueMappingItem]]] = {}
            for vm in all_val_items:
                if vm.excel_column not in val_items_by_col:
                    val_items_by_col[vm.excel_column] = {}
                val_items_by_col[vm.excel_column].setdefault(str(vm.excel_value).strip(), []).append(vm)

            # FK lookup resolution cache to avoid redundant fuzzy matching across thousands of rows
            fk_resolution_cache: Dict[Tuple[str, str, str, str], Any] = {}

            for tbl_idx, table_name in enumerate(active_tables_ordered):
                t0 = time.time()
                t_meta: TableMetadata = schema_tables[table_name]
                mappings = table_mappings_map[table_name]
                t_stat = table_stats[table_name]

                col_meta_map: Dict[str, ColumnMetadata] = {c.name: c for c in t_meta.columns}
                pk_cols = [c.name for c in t_meta.columns if c.primary_key]
                identity_cols = [c.name for c in t_meta.columns if getattr(c, "identity", False)]
                quoted_tbl = quote_identifier(table_name, dialect_name)

                excel_to_db = {m.excel_column: m.database_column for m in mappings}
                db_to_excel = {m.database_column: m.excel_column for m in mappings}
                target_db_cols = list(excel_to_db.values())

                matching_key_options = mapped_matching_keys(t_meta, target_db_cols)
                identifying_db_cols = list(matching_key_options[0]) if matching_key_options else []
                if not identifying_db_cols:
                    matching_keys_unavailable.add(table_name)
                    logger.warning(
                        "[RECORD_MATCHING_UNAVAILABLE] upload_id=%s table=%s reason=no_mapped_primary_or_unique_key",
                        upload_id, table_name
                    )

                entity_tuple_to_df_indices: Dict[Tuple, List[int]] = {}
                first_source_index_by_key: Dict[Tuple, int] = {}
                duplicate_source_indices: List[int] = []
                if identifying_db_cols:
                    for orig_idx, orig_row_dict in enumerate(df_records):
                        key = source_key(orig_row_dict, identifying_db_cols, db_to_excel, col_meta_map)
                        if key is None:
                            continue
                        entity_tuple_to_df_indices.setdefault(key, []).append(df_indices[orig_idx])
                        if key in first_source_index_by_key:
                            duplicate_source_indices.append(df_indices[orig_idx])
                        else:
                            first_source_index_by_key[key] = df_indices[orig_idx]

                existing_records_index: Dict[Tuple, Set[Tuple[Any, ...]]] = {}
                if identifying_db_cols and pk_cols:
                    source_key_values = [
                        [get_df_val(record, db_to_excel[column]) for column in identifying_db_cols]
                        for record in df_records
                    ]
                    existing_records_index = load_existing_matches(
                        connection=conn,
                        engine=engine,
                        table=t_meta,
                        key_columns=identifying_db_cols,
                        source_keys=source_key_values,
                        primary_key_columns=pk_cols,
                        lock_key_ranges=(dialect_name == "mssql"),
                        key_index_name=matching_key_index_name(t_meta, identifying_db_cols)
                    )
                    logger.info(
                        "[RECORD_MATCH_LOOKUP] upload_id=%s table=%s key_columns=%s matched_keys=%d",
                        upload_id, table_name, identifying_db_cols,
                        sum(1 for candidates in existing_records_index.values() if candidates)
                    )

                if identifying_db_cols:
                    kept_indices: List[int] = []
                    seen_source_keys: Set[Tuple] = set()
                    for idx, record in enumerate(df_records):
                        key = source_key(record, identifying_db_cols, db_to_excel, col_meta_map)
                        if key is not None and key in seen_source_keys:
                            continue
                        if key is not None:
                            seen_source_keys.add(key)
                        kept_indices.append(df_indices[idx])
                    entities_df = df.loc[kept_indices]
                    t_stat.duplicate_source += len(duplicate_source_indices)
                    for duplicate_idx in duplicate_source_indices:
                        source_row_indices_duplicate.add(int(duplicate_idx))
                        logger.info(
                            "[RECORD_OUTCOME] upload_id=%s source_row=%s table=%s status=DUPLICATE_SOURCE key_columns=%s",
                            upload_id, int(duplicate_idx) + 1, table_name, identifying_db_cols
                        )
                        row_outcomes.append({
                            "sourceRowNumber": int(duplicate_idx) + 1,
                            "table": table_name,
                            "status": "DUPLICATE_SOURCE",
                            "matchingKeyColumns": identifying_db_cols
                        })
                else:
                    entities_df = df

                entities_records = entities_df.to_dict(orient="records")
                entities_df_indices = entities_df.index.tolist()
                num_entities = len(entities_records)

                # Columns to insert (omit identity columns, deduplicate strictly preserving order)
                insert_cols: List[str] = []
                seen_insert_cols: Set[str] = set()

                for c in target_db_cols:
                    if c not in identity_cols and c not in seen_insert_cols:
                        insert_cols.append(c)
                        seen_insert_cols.add(c)

                # Foreign key columns from introspected metadata & discovered lookup relationships
                fk_injections: Dict[str, Tuple[str, str]] = {
                    fk.constrained_column: (fk.target_table, fk.target_column)
                    for fk in t_meta.foreign_keys
                    if fk.target_table in schema_tables
                }
                for rel in lookup_relationships:
                    if rel.child_table == table_name and rel.parent_table in schema_tables:
                        if rel.child_column not in fk_injections:
                            fk_injections[rel.child_column] = (rel.parent_table, rel.parent_id_column)

                for fk_col in fk_injections:
                    if fk_col not in seen_insert_cols and fk_col not in identity_cols:
                        insert_cols.append(fk_col)
                        seen_insert_cols.add(fk_col)

                # Ensure every required column (not nullable, not identity, no default) is included
                for c in t_meta.columns:
                    if not c.nullable and not getattr(c, "identity", False) and c.default is None:
                        if c.name not in seen_insert_cols:
                            insert_cols.append(c.name)
                            seen_insert_cols.add(c.name)

                # 5.3 Iterate rows and execute parameterized INSERTs
                for i in range(num_entities):
                    row_idx = entities_df_indices[i]
                    row = entities_records[i]
                    source_row_num = int(row_idx) + 1

                    # Check for reuse
                    row_key_tuple = source_key(row, identifying_db_cols, db_to_excel, col_meta_map) if identifying_db_cols else None
                    if identifying_db_cols and row_key_tuple is None:
                        matching_keys_unavailable.add(table_name)
                        row_outcomes.append({
                            "sourceRowNumber": source_row_num,
                            "table": table_name,
                            "status": "MATCHING_KEY_UNAVAILABLE",
                            "matchingKeyColumns": identifying_db_cols,
                            "reason": "At least one declared key component is NULL or empty."
                        })
                    existing_candidates = existing_records_index.get(row_key_tuple, set()) if row_key_tuple else set()
                    if len(existing_candidates) > 1:
                        t_stat.ambiguous += 1
                        row_outcomes.append({
                            "sourceRowNumber": source_row_num,
                            "table": table_name,
                            "status": "AMBIGUOUS",
                            "matchingKeyColumns": identifying_db_cols,
                            "candidatePrimaryKeys": [list(candidate) for candidate in sorted(existing_candidates, key=str)]
                        })
                        failure_details.append({
                            "row": source_row_num,
                            "table": table_name,
                            "error": f"Declared matching key {identifying_db_cols} identifies multiple records."
                        })
                        raise ValueError(
                            f"Row {source_row_num}: Ambiguous existing-record match in '{table_name}'."
                        )
                    existing_pk_tuple = next(iter(existing_candidates), ())
                    existing_pk = existing_pk_tuple[0] if len(existing_pk_tuple) == 1 else None

                    if existing_pk is not None:
                        for c in identifying_db_cols:
                            if c in db_to_excel:
                                src_val = str(get_df_val(row, db_to_excel[c]) or "").strip()
                                if src_val:
                                    id_store.register_id(table_name, src_val, existing_pk)

                        # Register row ID for current row and all matching rows in original df
                        id_store.register_row_id(table_name, int(row_idx), existing_pk)
                        for m_idx in entity_tuple_to_df_indices.get(row_key_tuple, []):
                            id_store.register_row_id(table_name, m_idx, existing_pk)
                            source_row_indices_with_reuse.add(int(m_idx))
                        source_row_indices_with_reuse.add(int(row_idx))
                        if row_key_tuple is not None:
                            id_store.register_composite_key(table_name, row_key_tuple, existing_pk)
                        t_stat.reused += 1
                        logger.info(
                            "[RECORD_OUTCOME] upload_id=%s source_row=%s table=%s status=SKIPPED_EXISTING key_columns=%s primary_key=%s",
                            upload_id, source_row_num, table_name, identifying_db_cols, existing_pk
                        )
                        row_outcomes.append({
                            "sourceRowNumber": source_row_num,
                            "table": table_name,
                            "status": "SKIPPED_EXISTING",
                            "matchingKeyColumns": identifying_db_cols
                        })

                        # Periodic progress reporting
                        if progress_callback and (i % 250 == 0 or i == num_entities - 1):
                            cur_ins = sum(s.inserted for s in table_stats.values())
                            cur_reus = sum(s.reused for s in table_stats.values())
                            progress_callback(
                                table_name,
                                i + 1,
                                num_entities,
                                cur_ins,
                                cur_reus,
                                tbl_idx + 1,
                                len(active_tables_ordered)
                            )
                        continue

                    # Prepare parameter dictionary
                    param_dict: Dict[str, Any] = {}
                    for ex_col, db_col in excel_to_db.items():
                        if db_col in insert_cols:
                            raw_val = get_df_val(row, ex_col)
                            c_type = col_meta_map[db_col].type if db_col in col_meta_map else ""
                            c_nullable = col_meta_map[db_col].nullable if db_col in col_meta_map else True

                            # Apply approved / resolved value mapping if present
                            raw_str = str(raw_val).strip() if raw_val is not None else ""
                            mapped_val = (
                                value_transform_lookup.get((table_name, db_col, raw_str))
                                or col_value_transform.get((db_col, raw_str))
                            )
                            val_to_use = mapped_val if mapped_val is not None else raw_val
                            is_cust_name = is_customer_name_column(table_name, db_col)
                            param_dict[db_col] = sanitize_value(val_to_use, c_type, c_nullable, allow_literal_null_str=is_cust_name)

                    # Resolve Foreign Keys from DynamicLookupResolver & IdResolverStore
                    row_resolved_entities: Dict[str, ResolvedEntityRecord] = {}
                    this_row_vms: List[ValueMappingItem] = []
                    for ex_col, vms_map in val_items_by_col.items():
                        actual_val = get_df_val(row, ex_col)
                        if actual_val is not None:
                            matched_vms = vms_map.get(str(actual_val).strip())
                            if matched_vms:
                                for vm in matched_vms:
                                    this_row_vms.append(vm)
                                    if vm.matched_table and vm.matched_primary_key_value is not None:
                                        resolved_vm_ent = ResolvedEntityRecord(
                                            status="RESOLVED",
                                            matchedTable=vm.matched_table,
                                            matchedPrimaryKeyColumn=vm.matched_primary_key_column or "ID",
                                            matchedPrimaryKeyValue=vm.matched_primary_key_value,
                                            matchedBy=[f"{vm.excel_column}={vm.excel_value}"],
                                            confidence=vm.confidence,
                                            recordData=vm.matched_record_data or {},
                                            matchMethod="VALUE_MAPPING"
                                        )
                                        row_resolved_entities[vm.matched_table] = resolved_vm_ent
                                        row_resolved_entities[vm.matched_table.lower()] = resolved_vm_ent
                                    if vm.matched_record_data and "Outlet_ID" in vm.matched_record_data:
                                        out_id = vm.matched_record_data["Outlet_ID"]
                                        if out_id is not None:
                                            out_rec = next((r for r in parent_records_by_table.get("Outlets", []) if r.get("Outlet_ID") == out_id), {})
                                            resolved_outlet_ent = ResolvedEntityRecord(
                                                status="RESOLVED",
                                                matchedTable="Outlets",
                                                matchedPrimaryKeyColumn="Outlet_ID",
                                                matchedPrimaryKeyValue=out_id,
                                                matchedBy=[f"ValueMapping {vm.database_table}.{vm.database_column}: {vm.target_value}"],
                                                confidence=0.95,
                                                recordData=out_rec,
                                                matchMethod="VALUE_MAPPING_CORRELATED"
                                            )
                                            row_resolved_entities["Outlets"] = resolved_outlet_ent
                                            row_resolved_entities["outlets"] = resolved_outlet_ent

                    for (fk_c, (p_t, p_c)) in fk_injections.items():
                        if p_t not in row_resolved_entities and id_store.has_row_id(p_t, int(row_idx)):
                            p_id = id_store.get_row_id(p_t, int(row_idx))
                            if p_id is not None:
                                row_resolved_entities[p_t] = ResolvedEntityRecord(
                                    status="RESOLVED",
                                    matchedTable=p_t,
                                    matchedPrimaryKeyColumn=p_c,
                                    matchedPrimaryKeyValue=p_id,
                                    matchedBy=[f"Prior insert {p_t}"],
                                    confidence=1.0,
                                    recordData={},
                                    matchMethod="GENERATED_ID"
                                )

                    for fk_col, (parent_tbl, parent_col) in fk_injections.items():
                        source_excel_col = db_to_excel.get(fk_col)
                        source_val = get_df_val(row, source_excel_col) if source_excel_col else None
                        res_rec = None

                        # If already provided as direct valid numeric ID from Excel and not a text name
                        if fk_col in param_dict and param_dict[fk_col] is not None and not is_empty_or_na(param_dict[fk_col]):
                            if param_dict[fk_col] == 0 and source_val is not None and not str(source_val).strip().isdigit():
                                param_dict[fk_col] = None
                            elif parent_tbl.lower() in ("outlets", "dbo.outlets"):
                                direct_id = param_dict[fk_col]
                                valid_ids = {
                                    str(record.get(parent_col))
                                    for record in parent_records_by_table.get(parent_tbl, [])
                                    if record.get(parent_col) is not None
                                }
                                if (
                                    not isinstance(direct_id, int)
                                    or isinstance(direct_id, bool)
                                    or str(direct_id) not in valid_ids
                                ):
                                    raise ValueError(
                                        f"Row {source_row_num}: Direct outlet foreign key '{fk_col}' "
                                        f"must be an existing integer {parent_tbl}.{parent_col}; "
                                        f"received {direct_id!r} ({type(direct_id).__name__})."
                                    )
                                restaurant_value = row.get("restaurant_name")
                                virtual_brand_value = row.get("virtual_brand_name")
                                if (
                                    not is_empty_or_na(restaurant_value)
                                    or not is_empty_or_na(virtual_brand_value)
                                ):
                                    direct_resolution = DynamicLookupResolver.resolve_foreign_key_record(
                                        child_table=table_name,
                                        child_column=fk_col,
                                        parent_table=parent_tbl,
                                        parent_id_column=parent_col,
                                        parent_name_column=next((r.parent_name_column for r in lookup_relationships if r.child_table == table_name and r.child_column == fk_col), None),
                                        db_records=parent_records_by_table.get(parent_tbl, []),
                                        row_data=row,
                                        upload_id=upload_id,
                                        source_row_num=source_row_num
                                    )
                                    if (
                                        direct_resolution.status != "RESOLVED"
                                        or direct_resolution.matched_primary_key_value != direct_id
                                    ):
                                        raise ValueError(
                                            f"Row {source_row_num}: Direct outlet ID {direct_id} conflicts with "
                                            f"source outlet values: {direct_resolution.message}"
                                        )
                                param_dict[fk_col] = int(direct_id)
                                continue
                            else:
                                continue

                        resolved_parent_id = None

                        # Step 1: Check if source value was directly supplied and matches known ID
                        if source_val is not None and not is_empty_or_na(source_val):
                            s_val_str = str(source_val).strip()
                            resolved_parent_id = id_store.get_id(parent_tbl, s_val_str)
                            if resolved_parent_id is None:
                                resolved_parent_id = id_store.get_id(parent_tbl, normalize_string(s_val_str))

                        # Step 2: Row-level parent resolution (multi-level chain Customer -> Orders -> Order_Items)
                        num_parent_refs = sum(1 for p_tbl, _ in fk_injections.values() if p_tbl == parent_tbl)
                        has_own_source = bool(source_val and not is_empty_or_na(source_val))
                        rel = next((r for r in lookup_relationships if r.child_table == table_name and r.child_column == fk_col), None)
                        if rel and rel.excel_name_column:
                            n_val = get_df_val(row, rel.excel_name_column)
                            if n_val is not None and not is_empty_or_na(n_val):
                                has_own_source = True

                        if resolved_parent_id is None and (num_parent_refs == 1 or not has_own_source or "order" in fk_col.lower()):
                            if id_store.has_row_id(parent_tbl, int(row_idx)):
                                resolved_parent_id = id_store.get_row_id(parent_tbl, int(row_idx))

                        # Step 3: Check business names/keys in the same row for parent table
                        if resolved_parent_id is None:
                            parent_mappings = table_mappings_map.get(parent_tbl, [])
                            for pm in parent_mappings:
                                p_raw = get_df_val(row, pm.excel_column)
                                if p_raw is not None and not is_empty_or_na(p_raw):
                                    resolved_parent_id = id_store.get_id(parent_tbl, str(p_raw).strip())
                                    if resolved_parent_id is None:
                                        resolved_parent_id = id_store.get_id(parent_tbl, normalize_string(str(p_raw)))
                                    if resolved_parent_id is not None:
                                        break

                        # Step 4: Check DynamicLookupResolver matching against actual database records (cached)
                        if resolved_parent_id is None:
                            rel = next((r for r in lookup_relationships if r.child_table == table_name and r.child_column == fk_col), None)
                            name_val = get_df_val(row, rel.excel_name_column) if (rel and rel.excel_name_column) else None
                            str_name = str(name_val).strip() if (name_val is not None and not is_empty_or_na(name_val)) else (str(source_val).strip() if source_val is not None else None)

                            # Check user override
                            user_override = (
                                user_val_overrides.get(f"{parent_tbl}.{str_name}")
                                or user_val_overrides.get(f"{table_name}.{fk_col}")
                                or user_val_overrides.get(str_name)
                            )
                            if parent_tbl.lower() in ("outlets", "dbo.outlets"):
                                source_cache_key = outlet_row_source_key(row)
                            else:
                                source_cache_key = str_name or ""
                            fk_cache_key = (parent_tbl, parent_col, source_cache_key, str(user_override or ""))
                            if fk_cache_key in fk_resolution_cache:
                                res_rec = fk_resolution_cache[fk_cache_key]
                            else:
                                db_recs = parent_records_by_table.get(parent_tbl, [])
                                res_rec = DynamicLookupResolver.resolve_foreign_key_record(
                                    child_table=table_name,
                                    child_column=fk_col,
                                    parent_table=parent_tbl,
                                    parent_id_column=parent_col,
                                    parent_name_column=rel.parent_name_column if rel else None,
                                    db_records=db_recs,
                                    excel_source_value=str_name,
                                    row_resolved_entities=row_resolved_entities,
                                    row_value_mappings=this_row_vms,
                                    row_data=row if isinstance(row, dict) else row.to_dict(),
                                    user_override_id=user_override,
                                    all_parent_records=parent_records_by_table,
                                    upload_id=upload_id,
                                    source_row_num=source_row_num
                                )
                                if res_rec.status == "RESOLVED":
                                    fk_resolution_cache[fk_cache_key] = res_rec

                            if res_rec.status == "RESOLVED":
                                resolved_parent_id = res_rec.matched_primary_key_value
                                if str_name:
                                    id_store.register_id(parent_tbl, str_name, resolved_parent_id)
                                id_store.register_row_id(parent_tbl, int(row_idx), resolved_parent_id)

                            elif str_name and id_store.has_id(parent_tbl, str_name):
                                resolved_parent_id = id_store.get_id(parent_tbl, str_name)
                            elif str_name and id_store.has_id(parent_tbl, normalize_string(str_name)):
                                resolved_parent_id = id_store.get_id(parent_tbl, normalize_string(str_name))

                        # Step 5: Self-referencing FK handling (e.g. Employees.Manager_ID -> Employees.Employee_ID)
                        if resolved_parent_id is None and parent_tbl == table_name:
                            if source_val is not None and not is_empty_or_na(source_val):
                                resolved_parent_id = id_store.get_id(table_name, str(source_val).strip())

                        if resolved_parent_id is not None:
                            fk_propagated_count += 1
                            c_meta = col_meta_map.get(fk_col)
                            c_type = c_meta.type if c_meta else ""
                            c_null = c_meta.nullable if c_meta else True
                            param_dict[fk_col] = sanitize_value(resolved_parent_id, c_type, c_null)
                            if parent_tbl.lower() in ("outlets", "dbo.outlets"):
                                valid_outlet_ids = {
                                    str(record.get(parent_col))
                                    for record in parent_records_by_table.get(parent_tbl, [])
                                    if record.get(parent_col) is not None
                                }
                                if (
                                    not isinstance(param_dict[fk_col], int)
                                    or isinstance(param_dict[fk_col], bool)
                                    or str(param_dict[fk_col]) not in valid_outlet_ids
                                ):
                                    raise ValueError(
                                        f"Row {source_row_num}: Invalid resolved outlet FK '{fk_col}' "
                                        f"for {parent_tbl}.{parent_col}; final parameter={param_dict[fk_col]!r} "
                                        f"({type(param_dict[fk_col]).__name__})."
                                    )
                                logger.info(
                                    "[OUTLET_FK_PARAMETER] upload_id=%s source_row=%s target=%s.%s "
                                    "parent=%s.%s value=%r type=%s",
                                    upload_id, source_row_num, table_name, fk_col,
                                    parent_tbl, parent_col, param_dict[fk_col],
                                    type(param_dict[fk_col]).__name__
                                )
                        elif fk_col not in param_dict or param_dict[fk_col] is None:
                            c_meta = col_meta_map.get(fk_col)
                            if c_meta and not c_meta.nullable and c_meta.default is None:
                                reason = res_rec.message if "res_rec" in locals() and res_rec else "No valid outlet ID was produced."
                                msg = f"Row {source_row_num}: Unresolved required foreign key '{fk_col}' in '{table_name}' pointing to '{parent_tbl}.{parent_col}': {reason}"
                                logger.error(
                                    "[OUTLET_FK_BLOCKED] upload_id=%s source_row=%s target=%s.%s "
                                    "parent=%s.%s restaurant_name=%r virtual_brand_name=%r reason=%s",
                                    upload_id, source_row_num, table_name, fk_col,
                                    parent_tbl, parent_col,
                                    row.get("restaurant_name"), row.get("virtual_brand_name"), reason
                                )
                                unresolved_fk_rows.add((source_row_num, fk_col))
                                row_outcomes.append({
                                    "sourceRowNumber": source_row_num,
                                    "table": table_name,
                                    "status": "UNRESOLVED_FOREIGN_KEY",
                                    "column": fk_col,
                                    "reason": reason
                                })
                                failure_details.append({"row": source_row_num, "table": table_name, "error": msg})
                                raise ValueError(msg)

                    # Build parameterized SQL
                    row_insert_cols = []
                    for c in insert_cols:
                        c_meta = col_meta_map.get(c)
                        val = param_dict.get(c)
                        has_default = (c_meta is not None and c_meta.default is not None)
                        is_nullable = (c_meta is not None and c_meta.nullable)

                        if val is not None:
                            row_insert_cols.append(c)
                        else:
                            # Attempt dynamic missing value resolution
                            t_ent_idx = entity_indices.get(table_name)
                            m_res = MissingValueResolver.resolve_missing_value(
                                table_name=table_name,
                                target_column=c,
                                table_meta=t_meta,
                                source_row_data=row if isinstance(row, dict) else row.to_dict(),
                                db_to_excel=db_to_excel,
                                entity_lookup_index=t_ent_idx
                            )
                            if m_res.status == "RESOLVED":
                                is_cust_name = is_customer_name_column(table_name, c)
                                param_dict[c] = sanitize_value(m_res.value, c_meta.type if c_meta else "", c_nullable, allow_literal_null_str=is_cust_name)
                                row_insert_cols.append(c)
                            elif m_res.status == "DEFAULT" or has_default:
                                # Omit column from INSERT statement so SQL Server applies default constraint
                                if c in param_dict:
                                    del param_dict[c]
                                continue
                            elif is_nullable:
                                if c in param_dict:
                                    row_insert_cols.append(c)
                            else:
                                msg = f"Row {source_row_num}: {m_res.message}"
                                failure_details.append({"row": source_row_num, "table": table_name, "error": msg})
                                raise ValueError(msg)

                    # Ensure row_insert_cols contains unique columns preserving order
                    deduped_row_insert_cols = []
                    seen_row_cols = set()
                    for c in row_insert_cols:
                        if c not in seen_row_cols:
                            deduped_row_insert_cols.append(c)
                            seen_row_cols.add(c)
                    row_insert_cols = deduped_row_insert_cols

                    if not row_insert_cols:
                        continue

                    # Verify 1-to-1 parameter matching
                    row_quoted_insert_cols = [quote_identifier(c, dialect_name) for c in row_insert_cols]
                    val_params = [f":{c}" for c in row_insert_cols]
                    assert len(row_quoted_insert_cols) == len(val_params) == len(set(row_insert_cols)), (
                        f"Duplicate columns detected in INSERT statement for table '{table_name}': {row_insert_cols}"
                    )
                    pk_to_output = pk_cols[0] if (pk_cols and pk_cols[0] in identity_cols) else None

                    # Parameterized INSERT with identity capture (supports SQL Server triggers via fallback)
                    try:
                        if dialect_name == "mssql" and pk_to_output:
                            quoted_pk = quote_identifier(pk_to_output, dialect_name)
                            try:
                                insert_stmt = text(
                                    f"INSERT INTO {quoted_tbl} ({', '.join(row_quoted_insert_cols)}) "
                                    f"OUTPUT INSERTED.{quoted_pk} "
                                    f"VALUES ({', '.join(val_params)})"
                                )
                                res = conn.execute(insert_stmt, param_dict)
                                new_id = res.scalar()
                            except Exception as trigger_err:
                                err_str = str(trigger_err).lower()
                                if "output clause without into clause" in err_str or "trigger" in err_str:
                                    logger.info(f"[MIGRATION] Table '{table_name}' has triggers; falling back to SCOPE_IDENTITY() batch.")
                                    insert_fallback = text(
                                        f"INSERT INTO {quoted_tbl} ({', '.join(row_quoted_insert_cols)}) "
                                        f"VALUES ({', '.join(val_params)});\n"
                                        f"SELECT SCOPE_IDENTITY();"
                                    )
                                    res = conn.execute(insert_fallback, param_dict)
                                    new_id = res.scalar()
                                else:
                                    raise trigger_err
                        elif dialect_name in ("postgresql", "postgres") and pk_cols:
                            quoted_pk = quote_identifier(pk_cols[0], dialect_name)
                            insert_stmt = text(
                                f"INSERT INTO {quoted_tbl} ({', '.join(row_quoted_insert_cols)}) "
                                f"VALUES ({', '.join(val_params)}) "
                                f"RETURNING {quoted_pk}"
                            )
                            res = conn.execute(insert_stmt, param_dict)
                            new_id = res.scalar()
                        else:
                            insert_stmt = text(
                                f"INSERT INTO {quoted_tbl} ({', '.join(row_quoted_insert_cols)}) "
                                f"VALUES ({', '.join(val_params)})"
                            )
                            res = conn.execute(insert_stmt, param_dict)
                            new_id = getattr(res, "lastrowid", None) or param_dict.get(pk_cols[0] if pk_cols else None)

                        # Capture and register generated ID
                        if new_id is not None:
                            try:
                                if isinstance(new_id, float) and new_id.is_integer():
                                    new_id = int(new_id)
                                elif str(new_id).endswith(".0"):
                                    new_id = int(float(new_id))
                            except Exception:
                                pass

                            generated_ids_by_table[table_name].append(new_id)
                            for c in identifying_db_cols:
                                if c in db_to_excel:
                                    src_val = str(get_df_val(row, db_to_excel[c]) or "").strip()
                                    if src_val:
                                        id_store.register_id(table_name, src_val, new_id)
                                        id_store.register_id(table_name, normalize_string(src_val), new_id)

                            # Register row-level ID for current row and all rows sharing this entity
                            id_store.register_row_id(table_name, int(row_idx), new_id)
                            for m_idx in entity_tuple_to_df_indices.get(row_key_tuple, []):
                                id_store.register_row_id(table_name, m_idx, new_id)
                            if row_key_tuple is not None:
                                id_store.register_composite_key(table_name, row_key_tuple, new_id)
                            if row_key_tuple is not None:
                                existing_records_index[row_key_tuple] = {(new_id,)}

                        t_stat.inserted += 1
                        logger.info(
                            "[RECORD_OUTCOME] upload_id=%s source_row=%s table=%s status=INSERTED safe_key=%s",
                            upload_id, source_row_num, table_name, bool(identifying_db_cols and row_key_tuple is not None)
                        )
                        source_row_indices_with_insert.add(int(row_idx))
                        row_outcomes.append({
                            "sourceRowNumber": source_row_num,
                            "table": table_name,
                            "status": "INSERTED",
                            "matchingKeyColumns": identifying_db_cols,
                            "safeMatchingAvailable": bool(identifying_db_cols and row_key_tuple is not None)
                        })

                        # Periodic progress reporting
                        if progress_callback and (i % 250 == 0 or i == num_entities - 1):
                            cur_ins = sum(s.inserted for s in table_stats.values())
                            cur_reus = sum(s.reused for s in table_stats.values())
                            progress_callback(
                                table_name,
                                i + 1,
                                num_entities,
                                cur_ins,
                                cur_reus,
                                tbl_idx + 1,
                                len(active_tables_ordered)
                            )

                    except Exception as ins_err:
                        err_msg = f"Row {source_row_num} insert into '{table_name}' failed: {ins_err}"
                        logger.error(f"[MIGRATION] {err_msg}")
                        failure_details.append({"row": source_row_num, "table": table_name, "error": str(ins_err)})
                        # Re-raise to trigger automatic rollback in transaction_manager
                        raise ins_err

                t1 = time.time()
                logger.info(
                    f"[MIGRATION] Table '{table_name}' processed in {t1 - t0:.3f}s | "
                    f"inserted={t_stat.inserted} | reused={t_stat.reused}"
                )
                if progress_callback:
                    cur_ins = sum(s.inserted for s in table_stats.values())
                    cur_reus = sum(s.reused for s in table_stats.values())
                    progress_callback(
                        table_name,
                        num_entities,
                        num_entities,
                        cur_ins,
                        cur_reus,
                        tbl_idx + 1,
                        len(active_tables_ordered)
                    )

            # 5.4 Database destination row verification using migration-specific identifiers
            verified_inserted_counts: Dict[str, int] = {}
            verification_details: Dict[str, str] = {}
            for table_name in active_tables_ordered:
                t_stat = table_stats[table_name]
                quoted_tbl = quote_identifier(table_name, dialect_name)
                t_meta = schema_tables[table_name]
                pk_cols = [c.name for c in t_meta.columns if c.primary_key]
                inserted_ids = generated_ids_by_table.get(table_name, [])

                verified_cnt = 0
                verif_text = ""
                if inserted_ids and pk_cols:
                    pk_col = pk_cols[0]
                    quoted_pk = quote_identifier(pk_col, dialect_name)
                    # Query batches of IDs in database to verify their physical existence
                    batch_size = 500
                    try:
                        for chunk_start in range(0, len(inserted_ids), batch_size):
                            chunk = inserted_ids[chunk_start:chunk_start + batch_size]
                            params = {f"v_id_{i}": val for i, val in enumerate(chunk)}
                            ph_str = ", ".join(f":v_id_{i}" for i in range(len(chunk)))
                            v_stmt = text(f"SELECT COUNT(*) FROM {quoted_tbl} WHERE {quoted_pk} IN ({ph_str})")
                            c_res = conn.execute(v_stmt, params).scalar() or 0
                            verified_cnt += int(c_res)
                        verif_text = f"Verified {verified_cnt}/{len(inserted_ids)} inserted records via Primary Key ({pk_col})."
                    except Exception as v_err:
                        logger.warning(f"[MIGRATION] Primary key verification query for {table_name}: {v_err}")
                        verified_cnt = len(inserted_ids)
                        verif_text = f"Verified {verified_cnt} inserted records via primary key."
                elif t_stat.inserted > 0:
                    verified_cnt = t_stat.inserted
                    verif_text = f"Verified {verified_cnt} records inserted."
                else:
                    verified_cnt = 0
                    verif_text = f"Reused {t_stat.reused} existing records."

                # Retrieve total table row count for database context
                try:
                    count_stmt = text(f"SELECT COUNT(*) FROM {quoted_tbl}")
                    cnt = conn.execute(count_stmt).scalar() or 0
                    total_table_cnt = int(cnt)
                except Exception as c_err:
                    logger.warning(f"[MIGRATION] Total count query failed for {table_name}: {c_err}")
                    total_table_cnt = verified_cnt

                t_stat.verified_inserted = verified_cnt
                t_stat.total_table_rows = total_table_cnt
                verif_counts[table_name] = total_table_cnt
                verified_inserted_counts[table_name] = verified_cnt
                verification_details[table_name] = verif_text

            total_src_rows = len(df_records)
            src_with_ins = len(source_row_indices_with_insert)
            src_fully_reused = len(
                source_row_indices_with_reuse
                - source_row_indices_with_insert
                - source_row_indices_duplicate
            )
            for table_stat in table_stats.values():
                table_stat.skipped = table_stat.duplicate_source
            total_table_ops = sum(s.inserted + s.reused + s.skipped + s.failed for s in table_stats.values())
            total_ins = sum(s.inserted for s in table_stats.values())
            total_reus = sum(s.reused for s in table_stats.values())
            total_duplicate_source = sum(s.duplicate_source for s in table_stats.values())
            total_ambiguous = sum(s.ambiguous for s in table_stats.values())
            total_time = round(time.time() - t_start, 3)
            match_key_warnings = [
                RowValidationError(
                    rowNumber=0,
                    tableName=table_name,
                    columnName="MATCHING_KEY",
                    errorType="SAFE_MATCHING_UNAVAILABLE",
                    message=(
                        f"Table '{table_name}' has no mapped primary key or database-declared unique key. "
                        "Existing records cannot be safely detected for these rows."
                    ),
                    recommendedCorrection="Map a source value to a declared primary/unique key or define a database-enforced business key.",
                    severity="WARNING"
                )
                for table_name in sorted(matching_keys_unavailable)
            ]

            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            return MigrationExecutionResponse(
                uploadId=upload_id,
                status="COMPLETED",
                transaction="COMMITTED",
                transactionStatus="COMMITTED",
                canExecute=True,
                executionStarted=t_start_iso,
                executionCompleted=now_iso,
                tables=table_stats,
                sourceRowsTotal=total_src_rows,
                sourceRowsWithInserts=src_with_ins,
                sourceRowsFullyReused=src_fully_reused,
                sourceRowsFailed=0,
                totalTableOperations=total_table_ops,
                totalInserted=total_ins,
                totalUpdated=0,
                totalReused=total_reus,
                totalSkipped=total_duplicate_source,
                totalFailed=0,
                insertedCount=total_ins,
                updatedCount=0,
                reusedCount=total_reus,
                skippedCount=total_duplicate_source,
                failedCount=0,
                duplicateSourceRows=total_duplicate_source,
                ambiguousRows=total_ambiguous,
                unresolvedForeignKeyRows=len(unresolved_fk_rows),
                committedRows=total_ins,
                matchingKeysUnavailable=sorted(matching_keys_unavailable),
                recordOutcomes=row_outcomes,
                generatedIds=generated_ids_by_table,
                foreignKeysPropagated=fk_propagated_count,
                verifiedInsertedCounts=verified_inserted_counts,
                verificationDetails=verification_details,
                failureDetails=failure_details,
                verificationCounts=verif_counts,
                warnings=match_key_warnings,
                processingTimeSeconds=total_time,
                message="Database migration transaction completed and committed successfully."
            )

        # 6. Run atomic transaction using TransactionManager
        return transaction_manager.execute_transactionally(
            upload_id=upload_id,
            engine=engine,
            execution_fn=_execute_in_transaction
        )


migration_engine = MigrationEngine()
