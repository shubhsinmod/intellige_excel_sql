import time
import math
from typing import List, Dict, Any, Optional, Set, Tuple
import pandas as pd
from sqlalchemy import text, Engine
from app.models.schema import DatabaseSchemaMetadata, TableMetadata, ColumnMetadata
from app.models.migration import (
    ColumnMappingResult,
    SourceRowPreviewItem,
    ForeignKeyPlanItem,
    PlannedOperationsSummary,
    ValidationSummary,
    ValueResolutionResult,
    ValueResolutionSummary,
    ValueMappingItem,
    ResolvedEntityRecord,
    RowValidationError
)
from app.migration.dependency import DependencyResolver
from app.migration.validator import MigrationValidator, is_empty_or_na, is_inferred_foreign_key
from app.mapping.matcher import normalize_string, resolve_unmapped_required_columns
from app.mapping.values import ValueMatcher
from app.migration.lookup_resolver import DynamicLookupResolver, LookupRelationship
from app.migration.outlet_matcher import (
    OutletMatcher,
    extract_outlet_base_name,
    outlet_row_source_key,
    outlet_row_source_values
)
from app.migration.missing_value_resolver import (
    MissingValueResolver,
    TableEntityLookupIndex,
    identify_table_lookup_columns,
    is_customer_name_column
)
from app.migration.record_matcher import (
    canonical_key,
    load_existing_records,
    mapped_matching_keys,
    source_key
)
from app.core.logging import logger



def quote_identifier(name: str, dialect_name: str) -> str:
    clean = str(name).strip().replace("[", "").replace("]", "").replace("`", "").replace('"', '')
    if dialect_name == "mssql":
        return f"[{clean}]"
    elif dialect_name in ("mysql", "mariadb"):
        return f"`{clean}`"
    else:
        return f'"{clean}"'


class PreviewResultTuple(tuple):
    """
    Backwards-compatible 6-tuple with value_resolution_summary and value_mappings properties.
    Allows unpacking as 6 items: (preview_rows, dep_plan, planned_ops, val_summary, order, val_res)
    while exposing value_resolution_summary and value_mappings as attributes.
    """
    def __new__(
        cls,
        preview_rows,
        dependency_plan,
        planned_ops,
        validation_summary,
        active_tables_ordered,
        value_resolutions,
        value_resolution_summary=None,
        value_mappings=None
    ):
        instance = super().__new__(
            cls,
            (preview_rows, dependency_plan, planned_ops, validation_summary, active_tables_ordered, value_resolutions)
        )
        instance.preview_rows = preview_rows
        instance.dependency_plan = dependency_plan
        instance.planned_operations = planned_ops
        instance.validation_summary = validation_summary
        instance.execution_order = active_tables_ordered
        instance.value_resolutions = value_resolutions
        instance.value_resolution_summary = value_resolution_summary
        instance.value_mappings = value_mappings or []
        return instance


class MigrationPreviewEngine:
    """
    High-Performance, Read-Only Migration Preview and Validation Engine.
    IMPORTANT: This engine NEVER executes any INSERT, UPDATE, or DELETE statements.
    Only read-only SELECT queries are used to inspect existing database reference records.
    Accurately tracks all 7 value resolution metrics and provides complete row-by-row previews.
    """

    @staticmethod
    def generate_preview(
        df: pd.DataFrame,
        file_name: str,
        schema: DatabaseSchemaMetadata,
        column_mappings: List[ColumnMappingResult],
        engine: Engine,
        user_value_overrides: Optional[Dict[str, Dict[str, str]]] = None
    ) -> PreviewResultTuple:

        t0 = time.time()
        logger.info("[PREVIEW] Generating complete migration preview plan (READ-ONLY)...")

        dialect_name = engine.name
        schema_tables = {t.name: t for t in schema.tables}
        db_name = schema.database_name or "default"

        # 1. Filter mapped columns targeting active schema tables
        matched_mappings = [
            m for m in column_mappings
            if m.status == "MATCHED" and m.database_table in schema_tables
        ]
        table_mappings: Dict[str, List[ColumnMappingResult]] = {}
        for m in matched_mappings:
            table_mappings.setdefault(m.database_table, []).append(m)

        # Dynamically resolve unmapped required columns from source dataset
        table_mappings = resolve_unmapped_required_columns(schema, table_mappings, df.columns.tolist())

        # Discover dynamic lookup relationships and load parent records
        all_potential_tables = list(table_mappings.keys())
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

        # Outlets is always a read-only reference table.
        active_target_tables = [
            t for t in table_mappings
            if t.lower() not in ("outlets", "dbo.outlets")
        ]

        # 2. Topological execution order strictly scoped to active target tables
        execution_order = DependencyResolver.resolve_execution_order(schema, target_tables=active_target_tables)
        active_tables_ordered = [t for t in execution_order if t in active_target_tables]
        for t in active_target_tables:
            if t not in active_tables_ordered:
                active_tables_ordered.append(t)

        t1 = time.time()
        logger.info(f"[STAGE] DEPENDENCY_ORDERING_COMPLETED | active_tables={len(active_tables_ordered)} | {t1 - t0:.3f}s")

        logger.info(f"[STAGE] FOREIGN_KEY_RESOLUTION_START | file={file_name}")
        t_fk_start = time.time()
        # Collect dynamic foreign key relationships via database metadata inspection scoped to active tables
        lookup_relationships: List[LookupRelationship] = DynamicLookupResolver.discover_relationships(
            schema=schema,
            df_columns=df.columns.tolist(),
            column_mappings=column_mappings,
            active_tables=active_tables_ordered
        )
        # Ensure any newly discovered relationships with unqueried parent tables have records loaded
        unloaded_rels = [rel for rel in lookup_relationships if rel.parent_table not in parent_records_by_table]
        if unloaded_rels:
            more_recs = DynamicLookupResolver.load_parent_records(
                engine=engine,
                schema=schema,
                relationships=unloaded_rels,
                df=df
            )
            parent_records_by_table.update(more_recs)

        t3 = time.time()
        total_recs = sum(len(r) for r in parent_records_by_table.values())
        logger.info(f"[STAGE] PARENT_RECORDS_LOADED | records={total_recs} | tables={len(parent_records_by_table)} | {t3 - t1:.3f}s")

        tables_to_inspect = list(active_tables_ordered)
        for rel in lookup_relationships:
            if rel.parent_table in schema_tables and rel.parent_table not in tables_to_inspect:
                tables_to_inspect.append(rel.parent_table)

        # 3. Read-only inspection of existing records in database (SELECT only)
        existing_records_by_table: Dict[str, Dict[str, Any]] = {}
        existing_keys_set: Dict[str, Set[str]] = {}
        entity_indices: Dict[str, TableEntityLookupIndex] = {}
        planning_records_by_table: Dict[str, List[Dict[str, Any]]] = {}
        matching_keys_unavailable: Set[str] = set()

        logger.info(f"[STAGE] EXISTING_RECORD_LOOKUP_START | tables={len(tables_to_inspect)}")
        t_exist_start = time.time()
        with engine.connect() as conn:
            for table_name in tables_to_inspect:
                t_meta = schema_tables[table_name]
                pk_cols = [c.name for c in t_meta.columns if c.primary_key]
                identifying_cols = identify_table_lookup_columns(t_meta)
                req_cols = [c.name for c in t_meta.columns if not c.nullable and not getattr(c, "identity", False)]
                lookup = {}
                key_set: Set[str] = set()
                records_list: List[Dict[str, Any]] = []

                if table_name in active_tables_ordered:
                    db_to_excel = {
                        mapping.database_column: mapping.excel_column
                        for mapping in table_mappings.get(table_name, [])
                        if mapping.excel_column in df.columns
                    }
                    key_options = mapped_matching_keys(t_meta, db_to_excel)
                    key_columns = list(key_options[0]) if key_options else []
                    if not key_columns:
                        matching_keys_unavailable.add(table_name)
                    elif pk_cols:
                        source_records = df.to_dict(orient="records")
                        source_key_values = [
                            [record.get(db_to_excel[column]) for column in key_columns]
                            for record in source_records
                        ]
                        select_cols = list(dict.fromkeys([*key_columns, *pk_cols, *identifying_cols, *req_cols]))
                        try:
                            records_list = load_existing_records(
                                connection=conn,
                                engine=engine,
                                table=t_meta,
                                key_columns=key_columns,
                                source_keys=source_key_values,
                                primary_key_columns=pk_cols,
                                additional_columns=select_cols
                            )
                        except Exception as select_err:
                            matching_keys_unavailable.add(table_name)
                            logger.warning(
                                "[PREVIEW_RECORD_MATCH_LOOKUP_FAILED] table=%s key_columns=%s error=%s",
                                table_name, key_columns, select_err
                            )
                else:
                    records_list = parent_records_by_table.get(table_name, [])

                planning_records_by_table[table_name] = records_list
                for record in records_list:
                    pk_val = record.get(pk_cols[0]) if pk_cols else None
                    for column in list(dict.fromkeys([*identifying_cols, *pk_cols, *req_cols])):
                        raw_value = record.get(column)
                        if raw_value is None:
                            continue
                        value = str(raw_value).strip()
                        if value:
                            lookup.setdefault(value, pk_val if pk_val is not None else value)
                            lookup.setdefault(normalize_string(value), pk_val if pk_val is not None else value)
                            key_set.add(value)
                            key_set.add(normalize_string(value))

                existing_records_by_table[table_name] = lookup
                existing_keys_set[table_name] = key_set
                entity_indices[table_name] = TableEntityLookupIndex(records_list, identifying_cols)

        t4 = time.time()
        logger.info(f"[STAGE] EXISTING_RECORD_LOOKUP_COMPLETED | tables={len(tables_to_inspect)} | {t4 - t_exist_start:.3f}s")

        # 4. Plan table operations (Inserts vs Reused/Skipped) using fast vector deduplication
        planned_inserts_by_table: Dict[str, int] = {}
        planned_skips_by_table: Dict[str, int] = {}
        pending_inserts_by_table: Dict[str, Set[str]] = {}

        total_values_matched_existing = 0
        total_new_parents_required = 0
        existing_rows_skipped = 0
        duplicate_source_rows = 0
        ambiguous_key_errors: List[RowValidationError] = []
        source_records = df.to_dict(orient="records")

        for table_name in active_tables_ordered:
            mappings = table_mappings[table_name]
            db_to_excel = {m.database_column: m.excel_column for m in mappings if m.excel_column in df.columns}
            t_meta = schema_tables[table_name]

            pk_cols = [c.name for c in t_meta.columns if c.primary_key]
            key_options = mapped_matching_keys(t_meta, db_to_excel.keys())
            key_columns = list(key_options[0]) if key_options else []
            pending_set: Set[str] = set()
            matched_key_records: Dict[Tuple, Set[Tuple[Any, ...]]] = {}
            if key_columns and pk_cols:
                key_values = [
                    [row.get(db_to_excel[column]) for column in key_columns]
                    for row in source_records
                ]
                try:
                    key_metadata = {column.name: column for column in t_meta.columns}
                    for record in planning_records_by_table.get(table_name, []):
                        key = canonical_key([record.get(column) for column in key_columns], [key_metadata[column] for column in key_columns])
                        if key is not None:
                            matched_key_records.setdefault(key, set()).add(tuple(record.get(column) for column in pk_cols))
                except Exception as matching_error:
                    matching_keys_unavailable.add(table_name)
                    logger.warning(
                        "[PREVIEW_RECORD_MATCH_LOOKUP_FAILED] table=%s key_columns=%s error=%s",
                        table_name, key_columns, matching_error
                    )
            else:
                matching_keys_unavailable.add(table_name)

            first_source_row_by_key: Dict[Tuple, int] = {}
            inserts = 0
            skips = 0
            for row_index, row_data in enumerate(source_records):
                key = source_key(row_data, key_columns, db_to_excel, {column.name: column for column in t_meta.columns}) if key_columns else None
                if key is None:
                    inserts += 1
                    if key_columns:
                        matching_keys_unavailable.add(table_name)
                    continue
                if key in first_source_row_by_key:
                    skips += 1
                    duplicate_source_rows += 1
                    continue
                first_source_row_by_key[key] = row_index
                candidates = matched_key_records.get(key, set())
                if len(candidates) > 1:
                    skips += 1
                    ambiguous_key_errors.append(RowValidationError(
                        rowNumber=row_index + 1,
                        tableName=table_name,
                        columnName=", ".join(key_columns),
                        errorType="AMBIGUOUS_RECORD_MATCH",
                        message=f"Row {row_index + 1}: Declared matching key {key_columns} matches multiple database records.",
                        recommendedCorrection="Resolve duplicate records in the target database before migrating this row.",
                        severity="ERROR"
                    ))
                    continue
                if candidates:
                    skips += 1
                    existing_rows_skipped += 1
                    total_values_matched_existing += 1
                else:
                    inserts += 1
                    for key_column in key_columns:
                        raw_value = row_data.get(db_to_excel[key_column])
                        if not is_empty_or_na(raw_value):
                            value = str(raw_value).strip()
                            pending_set.add(value)
                            pending_set.add(normalize_string(value))

            planned_inserts_by_table[table_name] = inserts
            planned_skips_by_table[table_name] = skips
            if table_name in ("Outlets", "Customer"):
                total_new_parents_required += inserts

            pending_inserts_by_table[table_name] = pending_set

        t5 = time.time()
        logger.info(f"[PREVIEW] Stage 6: Planned table operations in {t5 - t4:.3f}s")

        # 5. Intelligent Value Matching across mapped categorical/status/type columns
        # (Evaluated before FK resolution so matched entities can be reused downstream)
        all_value_mappings: List[ValueMappingItem] = []
        value_transform_lookup: Dict[Tuple[str, str, str], str] = {}  # (table, col, raw_val) -> target_val
        col_value_transform: Dict[Tuple[str, str], str] = {}          # (col, raw_val) -> target_val
        valid_db_vals_cache: Dict[Tuple[str, str], List[str]] = {}

        CATEGORICAL_KEYWORDS = ("type", "status", "mode", "category", "channel", "method", "state", "brand", "source", "reason", "payment", "location")
        EXCLUDED_KEYWORDS = ("date", "time", "phone", "comment", "note", "desc", "bill", "guid", "price", "amount", "tax", "rate", "cost", "qty", "quantity")

        for m in matched_mappings:
            if m.excel_column in df.columns and m.database_table in schema_tables:
                col_lower = m.database_column.lower()
                is_categorical = any(k in col_lower for k in CATEGORICAL_KEYWORDS) or any(k in m.excel_column.lower() for k in CATEGORICAL_KEYWORDS)
                if not is_categorical and not any(k in col_lower for k in EXCLUDED_KEYWORDS):
                    nunique_cnt = df[m.excel_column].nunique(dropna=True)
                    if nunique_cnt <= 30:
                        is_categorical = True

                if is_categorical:
                    cache_key = (m.database_table, m.database_column)
                    if cache_key not in valid_db_vals_cache:
                        valid_db_vals_cache[cache_key] = ValueMatcher.get_valid_column_values(
                            engine=engine,
                            table=m.database_table,
                            column=m.database_column
                        )
                    valid_db_vals = valid_db_vals_cache[cache_key]

                    overrides_map = (user_value_overrides or {}).get(f"{m.database_table}.{m.database_column}") or (user_value_overrides or {}).get(m.excel_column)
                    db_recs_for_matching = parent_records_by_table.get(m.database_table, [])
                    items = ValueMatcher.match_column_values(
                        df=df,
                        excel_column=m.excel_column,
                        target_table=m.database_table,
                        target_column=m.database_column,
                        valid_db_values=valid_db_vals,
                        user_overrides=overrides_map,
                        database_scope=db_name,
                        db_records=db_recs_for_matching
                    )
                    for item in items:
                        all_value_mappings.append(item)
                        if item.target_value and item.status in ("MATCHED", "APPROVED"):
                            value_transform_lookup[(m.database_table, m.database_column, item.excel_value)] = item.target_value
                            col_value_transform[(m.database_column, item.excel_value)] = item.target_value

        t6 = time.time()
        logger.info(f"[PREVIEW] Stage 7: Intelligent value matching evaluated ({len(all_value_mappings)} items) in {t6 - t5:.3f}s")

        # 6. Dependency & Foreign-Key Plan (Reusing matched database records & dynamic resolution)
        fk_migration_cache: Dict[Any, ResolvedEntityRecord] = {}
        dependency_plan: List[ForeignKeyPlanItem] = []
        value_resolutions: List[ValueResolutionResult] = []

        for rel in lookup_relationships:
            if rel.child_table not in active_tables_ordered or rel.parent_table not in schema_tables:
                continue

            parent_existing = existing_records_by_table.get(rel.parent_table, {})
            parent_pending = pending_inserts_by_table.get(rel.parent_table, set())
            db_records = parent_records_by_table.get(rel.parent_table, [])
            ins_order = (active_tables_ordered.index(rel.child_table) + 1) if rel.child_table in active_tables_ordered else 1

            source_col = rel.excel_name_column or rel.excel_id_column
            sample_items = []
            if rel.parent_table.lower() in ("outlets", "dbo.outlets"):
                seen_outlet_keys = set()
                for row_idx, row_series in df.iterrows():
                    row_data = row_series.to_dict()
                    outlet_key = outlet_row_source_key(row_data)
                    if outlet_key == ("", "") or outlet_key in seen_outlet_keys:
                        continue
                    seen_outlet_keys.add(outlet_key)
                    restaurant_value, virtual_brand_value = outlet_row_source_values(row_data)
                    source_value = restaurant_value or virtual_brand_value
                    sample_items.append((str(source_value).strip() if source_value is not None else "", row_data, int(row_idx) + 1))
                sample_items = sample_items[:100]
            elif source_col and source_col in df.columns:
                sample_items = [
                    (str(value).strip(), None, None)
                    for value in df[source_col].dropna().unique()
                    if not is_empty_or_na(value)
                ][:20]

            if not sample_items:
                sample_items = [("(default / auto-linked)", None, None)]

            parent_idx = DynamicLookupResolver.get_or_create_index(
                db_records=db_records,
                parent_id_column=rel.parent_id_column,
                parent_name_column=rel.parent_name_column or rel.parent_id_column
            )

            for val, source_row_data, source_row_num in sample_items:
                if val == "(default / auto-linked)":
                    # Attempt to resolve default / auto-linked FK via matched entities or database default
                    res_rec = DynamicLookupResolver.resolve_foreign_key_record(
                        child_table=rel.child_table,
                        child_column=rel.child_column,
                        parent_table=rel.parent_table,
                        parent_id_column=rel.parent_id_column,
                        parent_name_column=rel.parent_name_column or rel.parent_id_column,
                        db_records=db_records,
                        excel_source_value=None,
                        row_value_mappings=all_value_mappings,
                        all_parent_records=parent_records_by_table,
                        index=parent_idx,
                        migration_cache=fk_migration_cache,
                        row_data=source_row_data,
                        source_row_num=source_row_num
                    )

                    if res_rec.status == "RESOLVED":
                        dependency_plan.append(ForeignKeyPlanItem(
                            childTable=rel.child_table,
                            childColumn=rel.child_column,
                            parentTable=rel.parent_table,
                            parentColumn=rel.parent_id_column,
                            sourceBusinessKey=None,
                            existingParentFound=True,
                            resolvedId=str(res_rec.matched_primary_key_value),
                            generatedIdPending=False,
                            status="MATCHED",
                            insertionOrder=ins_order,
                            matchedDatabaseName=str(res_rec.record_data.get(rel.parent_name_column or "") or res_rec.matched_primary_key_value),
                            valueSource="DATABASE_LOOKUP",
                            validationStatus="VALID",
                            candidates=[]
                        ))
                    else:
                        is_pending = rel.parent_table in active_tables_ordered
                        dependency_plan.append(ForeignKeyPlanItem(
                            childTable=rel.child_table,
                            childColumn=rel.child_column,
                            parentTable=rel.parent_table,
                            parentColumn=rel.parent_id_column,
                            sourceBusinessKey=None,
                            existingParentFound=False,
                            resolvedId=None,
                            generatedIdPending=is_pending,
                            status="PENDING" if is_pending else "NOT_FOUND",
                            insertionOrder=ins_order,
                            matchedDatabaseName=None,
                            valueSource="GENERATED_ID" if is_pending else "DATABASE_DEFAULT",
                            validationStatus="VALID" if is_pending else ("ERROR" if rel.is_required else "WARNING"),
                            candidates=[]
                        ))
                    continue

                # Resolve FK using resolve_foreign_key_record
                res_rec = DynamicLookupResolver.resolve_foreign_key_record(
                    child_table=rel.child_table,
                    child_column=rel.child_column,
                    parent_table=rel.parent_table,
                    parent_id_column=rel.parent_id_column,
                    parent_name_column=rel.parent_name_column or rel.parent_id_column,
                    db_records=db_records,
                    excel_source_value=val,
                    row_value_mappings=all_value_mappings,
                    all_parent_records=parent_records_by_table,
                    index=parent_idx,
                    migration_cache=fk_migration_cache,
                    row_data=source_row_data,
                    source_row_num=source_row_num
                )

                is_found = (res_rec.status == "RESOLVED")
                is_pending = (not is_found) and (val in parent_pending or rel.parent_table in active_tables_ordered)

                if is_found:
                    plan_status = "MATCHED"
                    v_status = "VALID"
                    res_src = "DATABASE_LOOKUP"
                    res_id_str = str(res_rec.matched_primary_key_value)
                    matched_name = str(res_rec.record_data.get(rel.parent_name_column or "") or res_rec.matched_primary_key_value)
                elif is_pending:
                    plan_status = "PENDING"
                    v_status = "VALID"
                    res_src = "GENERATED_ID"
                    res_id_str = None
                    matched_name = None
                elif res_rec.status == "AMBIGUOUS":
                    plan_status = "AMBIGUOUS"
                    v_status = "ERROR" if rel.is_required else "WARNING"
                    res_src = "AMBIGUOUS"
                    res_id_str = None
                    matched_name = None
                else:
                    plan_status = "NOT_FOUND"
                    v_status = "ERROR" if rel.is_required else "WARNING"
                    res_src = "UNRESOLVED"
                    res_id_str = None
                    matched_name = None

                dependency_plan.append(ForeignKeyPlanItem(
                    childTable=rel.child_table,
                    childColumn=rel.child_column,
                    parentTable=rel.parent_table,
                    parentColumn=rel.parent_id_column,
                    sourceBusinessKey=val,
                    existingParentFound=is_found,
                    resolvedId=res_id_str,
                    generatedIdPending=is_pending,
                    status=plan_status,
                    insertionOrder=ins_order,
                    matchedDatabaseName=matched_name,
                    valueSource=res_src,
                    validationStatus=v_status,
                    candidates=res_rec.candidates
                ))

                if source_col:
                    value_resolutions.append(ValueResolutionResult(
                        excelColumn=source_col,
                        excelValue=val,
                        referenceTable=rel.parent_table,
                        targetColumn=rel.parent_id_column,
                        resolvedId=res_id_str,
                        status=plan_status,
                        excelName=val,
                        matchedDatabaseName=matched_name,
                        valueSource=res_src,
                        validationStatus=v_status,
                        candidates=res_rec.candidates,
                        message=res_rec.message
                    ))

        t7 = time.time()
        logger.info(f"[STAGE] FOREIGN_KEY_RESOLUTION_COMPLETED | items={len(dependency_plan)} | {t7 - t_fk_start:.3f}s")

        # 7. Full Dataset Validation across entire dataset
        logger.info(f"[STAGE] ROW_VALIDATION_START | rows={len(df)}")
        t_val_start = time.time()
        validation_summary = MigrationValidator.validate_dataset(
            df=df,
            schema=schema,
            column_mappings=column_mappings,
            existing_lookup=existing_keys_set,
            parent_pending_inserts=pending_inserts_by_table,
            value_mappings=all_value_mappings,
            foreign_key_plans=dependency_plan,
            entity_indices=entity_indices,
            lookup_relationships=lookup_relationships,
            parent_records_by_table=parent_records_by_table
        )
        validation_summary.errors.extend(ambiguous_key_errors)
        for table_name in sorted(matching_keys_unavailable):
            validation_summary.warnings.append(RowValidationError(
                rowNumber=0,
                tableName=table_name,
                columnName="MATCHING_KEY",
                errorType="SAFE_MATCHING_UNAVAILABLE",
                message=f"No mapped primary key or database-declared unique key is available for safe repeat matching in '{table_name}'.",
                recommendedCorrection="Map a database-enforced primary/unique key for this table.",
                severity="WARNING"
            ))
        if ambiguous_key_errors:
            validation_summary.invalid_rows = len({error.row_number for error in validation_summary.errors if error.row_number > 0})
            validation_summary.valid_rows = max(0, validation_summary.total_rows - validation_summary.invalid_rows)
            validation_summary.can_execute = False

        t9 = time.time()
        logger.info(f"[STAGE] ROW_VALIDATION_COMPLETED | valid_rows={validation_summary.valid_rows} | errors={len(validation_summary.errors)} | {t9 - t_val_start:.3f}s")

        # 7. Accurate Calculation of All 7 Value Resolution Metrics
        # (1) Values inspected: total non-null cell values across confirmed mapped columns
        values_inspected_cnt = sum(
            int((~df[m.excel_column].isna() & ~df[m.excel_column].astype(str).str.strip().isin(["", "nan", "none", "null", "nat"])).sum())
            for m in matched_mappings if m.excel_column in df.columns
        )

        # (2) Values matched to existing database records
        # Count rows where business entity already exists
        values_matched_existing_cnt = sum(
            planned_skips_by_table.get(t, 0) for t in ("Outlets", "Customer", "Bills", "Order")
        )

        # (3) Values rejected by validation
        values_rejected_cnt = len(validation_summary.errors)

        # (4) Values resolved through confirmed mappings
        values_resolved_confirmed_cnt = max(0, values_inspected_cnt - values_rejected_cnt)

        # (5) Values requiring a new parent record
        values_requiring_new_parent_cnt = total_new_parents_required

        # (6) Unresolved foreign-key references
        unresolved_fks_cnt = sum(1 for item in dependency_plan if item.status == "NOT_FOUND")

        # (7) Ambiguous values requiring user review
        ambiguous_cols = [m.excel_column for m in column_mappings if m.status == "AMBIGUOUS"]
        ambiguous_values_cnt = sum(
            int((~df[col].isna() & ~df[col].astype(str).str.strip().isin(["", "nan", "none", "null", "nat"])).sum())
            for col in ambiguous_cols if col in df.columns
        )
        # Add ambiguous cell values from value matching
        ambiguous_val_occurrences = sum(item.occurrence_count for item in all_value_mappings if item.status == "AMBIGUOUS")
        ambiguous_values_cnt += ambiguous_val_occurrences

        value_resolution_summary = ValueResolutionSummary(
            valuesInspected=values_inspected_cnt,
            valuesMatchedExisting=values_matched_existing_cnt,
            valuesResolvedConfirmed=values_resolved_confirmed_cnt,
            valuesRequiringNewParent=values_requiring_new_parent_cnt,
            unresolvedForeignKeys=unresolved_fks_cnt,
            ambiguousValues=ambiguous_values_cnt,
            valuesRejectedValidation=values_rejected_cnt
        )

        # 8. Generate Row-Level Migration Preview Items (First 100 rows for crisp frontend display)
        preview_limit = min(len(df), 100)
        logger.info(f"[STAGE] SQL_PREVIEW_START | rows={preview_limit}")
        t_prev_start = time.time()
        preview_rows: List[SourceRowPreviewItem] = []

        for idx in range(preview_limit):
            row = df.iloc[idx]
            source_row_num = idx + 1
            row_resolved_entities: Dict[str, ResolvedEntityRecord] = {}

            # Collect active value mappings for this row and pre-register any resolved parent entities
            this_row_vms: List[ValueMappingItem] = []
            for vm in all_value_mappings:
                if vm.excel_column in df.columns:
                    cell_val = row.get(vm.excel_column)
                    if cell_val is not None and str(cell_val).strip() == vm.excel_value:
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
                        # If matched record in Customer has an Outlet_ID foreign key
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

            for table_name in active_tables_ordered:
                t_meta = schema_tables[table_name]
                col_meta_map = {c.name: c for c in t_meta.columns}
                mappings = table_mappings[table_name]
                excel_to_db = {m.excel_column: m.database_column for m in mappings if m.excel_column in df.columns}
                mapping_dict = {m.database_column: m for m in mappings}

                # Include all mapped columns plus identity, foreign-key, and mandatory columns
                included_cols = set(excel_to_db.values())
                for c in t_meta.columns:
                    if getattr(c, "identity", False):
                        included_cols.add(c.name)
                    if (
                        any(fk.constrained_column == c.name for fk in t_meta.foreign_keys)
                        or bool(getattr(c, "foreign_keys", []))
                        or is_inferred_foreign_key(c.name, active_target_tables)
                    ):
                        included_cols.add(c.name)
                    if not c.nullable and not getattr(c, "identity", False) and c.default is None:
                        included_cols.add(c.name)

                for col_name in sorted(included_cols):
                    col_meta = col_meta_map.get(col_name)
                    if not col_meta:
                        continue

                    src_col = next((ex for ex, db in excel_to_db.items() if db == col_name), None)
                    raw_val = row.get(src_col) if src_col else None
                    str_src_val = str(raw_val).strip() if (raw_val is not None and not is_empty_or_na(raw_val)) else None

                    is_ident = getattr(col_meta, "identity", False)
                    has_def = col_meta.default is not None
                    is_req = (not col_meta.nullable) and (not is_ident) and (not has_def)

                    final_val: Optional[str] = None
                    val_src = "EXCEL"
                    val_status = "VALID"
                    val_msg: Optional[str] = None

                    # Check Foreign Key relationship
                    rel = next((r for r in lookup_relationships if r.child_table == table_name and r.child_column == col_name), None)
                    is_fk = bool(
                        rel
                        or any(fk.constrained_column == col_name for fk in t_meta.foreign_keys)
                        or is_inferred_foreign_key(col_name, active_target_tables)
                    )

                    # Check Identity column
                    if is_ident:
                        final_val = "Generated by SQL Server"
                        val_src = "IDENTITY"

                    elif is_fk:
                        val_src = "FOREIGN_KEY"
                        src_lookup_val = None
                        if rel and rel.excel_name_column and rel.excel_name_column in df.columns:
                            raw_name_val = row.get(rel.excel_name_column)
                            if raw_name_val is not None and not is_empty_or_na(raw_name_val):
                                src_lookup_val = str(raw_name_val).strip()
                        elif str_src_val is not None:
                            src_lookup_val = str_src_val

                        user_override_val = None
                        if user_value_overrides:
                            user_override_val = (
                                user_value_overrides.get(f"{table_name}.{col_name}", {}).get(src_lookup_val or "")
                                or user_value_overrides.get(col_name, {}).get(src_lookup_val or "")
                            )

                        parent_t = rel.parent_table if rel else (
                            next((fk.target_table for fk in t_meta.foreign_keys if fk.constrained_column == col_name), None)
                            or (col_name[:-3] if col_name.lower().endswith("_id") else col_name)
                        )
                        parent_id_c = rel.parent_id_column if rel else (
                            next((fk.target_column for fk in t_meta.foreign_keys if fk.constrained_column == col_name), None)
                            or col_name
                        )
                        parent_name_c = rel.parent_name_column if rel else None
                        db_recs = parent_records_by_table.get(parent_t, [])

                        res_rec = DynamicLookupResolver.resolve_foreign_key_record(
                            child_table=table_name,
                            child_column=col_name,
                            parent_table=parent_t,
                            parent_id_column=parent_id_c,
                            parent_name_column=parent_name_c,
                            db_records=db_recs,
                            excel_source_value=src_lookup_val,
                            row_resolved_entities=row_resolved_entities,
                            row_value_mappings=this_row_vms,
                            row_data=row.to_dict(),
                            user_override_id=user_override_val,
                            migration_cache=fk_migration_cache,
                            all_parent_records=parent_records_by_table
                        )

                        if res_rec.status == "RESOLVED":
                            final_val = str(res_rec.matched_primary_key_value)
                            val_src = "DATABASE_LOOKUP"
                            val_status = "VALID"
                            val_msg = None
                        elif res_rec.status == "AMBIGUOUS":
                            final_val = None
                            val_src = "AMBIGUOUS"
                            val_status = "ERROR" if is_req else "WARNING"
                            val_msg = res_rec.message
                        elif (rel and rel.parent_table in active_tables_ordered) or (parent_t and parent_t in active_tables_ordered):
                            final_val = "Generated by SQL Server (pending insertion)"
                            val_src = "GENERATED_ID"
                            val_status = "VALID"
                        elif has_def:
                            final_val = col_meta.default or "Database Default"
                            val_src = "DATABASE_DEFAULT"
                            val_status = "VALID"
                        elif col_meta.nullable:
                            final_val = "NULL"
                            val_src = "NULL"
                            val_status = "VALID"
                        else:
                            final_val = None
                            val_src = "UNRESOLVED"
                            val_status = "ERROR"
                            val_msg = res_rec.message or f"Required foreign key '{col_name}' could not be resolved."

                    # Regular column
                    elif str_src_val is not None:
                        mapped_val = value_transform_lookup.get((table_name, col_name, str_src_val)) or col_value_transform.get((col_name, str_src_val))
                        if mapped_val is not None:
                            final_val = mapped_val
                            val_src = "VALUE_MAPPING" if mapped_val != str_src_val else "EXCEL"
                        else:
                            # If target column is date/time, format using shared normalize_date_value
                            col_t_up = col_meta.type.upper() if col_meta else ""
                            if any(d in col_t_up for d in ("DATE", "DATETIME", "DATETIME2", "SMALLDATETIME")):
                                from app.utils.normalization import normalize_date_value
                                norm_d, _, is_vd = normalize_date_value(raw_val, target_type=col_t_up)
                                final_val = norm_d if (is_vd and norm_d is not None) else str_src_val
                            else:
                                final_val = str_src_val
                            val_src = "EXCEL"
                    else:
                        t_ent_idx = entity_indices.get(table_name)
                        m_res = MissingValueResolver.resolve_missing_value(
                            table_name=table_name,
                            target_column=col_name,
                            table_meta=t_meta,
                            source_row_data=row.to_dict(),
                            db_to_excel={m.database_column: m.excel_column for m in mappings if m.excel_column in df.columns},
                            entity_lookup_index=t_ent_idx
                        )
                        if m_res.status == "RESOLVED":
                            final_val = str(m_res.value)
                            val_src = m_res.value_source
                            val_status = "VALID"
                            val_msg = m_res.message
                        elif m_res.status == "DEFAULT":
                            final_val = str(m_res.value) if m_res.value is not None else (col_meta.default or "Database Default")
                            val_src = "DATABASE_DEFAULT"
                            val_status = "VALID"
                            val_msg = m_res.message
                        elif m_res.status == "NULLABLE":
                            final_val = "NULL"
                            val_src = "NULL"
                            val_status = "VALID"
                        else:
                            final_val = None
                            val_src = "UNRESOLVED"
                            val_status = "ERROR" if is_req else "WARNING"
                            val_msg = m_res.message

                    m_info = mapping_dict.get(col_name)
                    m_status = m_info.status if m_info else ("SYSTEM" if is_ident else "UNMAPPED")
                    m_conf = m_info.confidence if m_info else 1.0

                    if is_ident:
                        req_type = "AUTO_GENERATED"
                    elif has_def:
                        req_type = "DATABASE_DEFAULT"
                    elif val_src in ("UNRESOLVED", "AMBIGUOUS") or (is_req and (final_val is None or val_status == "ERROR")):
                        req_type = "UNRESOLVED"
                    elif is_fk:
                        req_type = "FOREIGN_KEY"
                    elif is_req:
                        req_type = "MANDATORY"
                    else:
                        req_type = "OPTIONAL"

                    preview_rows.append(SourceRowPreviewItem(
                        sourceRow=source_row_num,
                        sourceFile=file_name,
                        targetDatabase=db_name,
                        targetSchema=getattr(t_meta, "schema_name", "dbo"),
                        targetTable=table_name,
                        targetColumn=col_name,
                        sourceColumn=src_col,
                        sourceValue=str_src_val,
                        finalValue=final_val,
                        valueSource=val_src,
                        mappingStatus=m_status,
                        mappingConfidence=m_conf,
                        isNullable=col_meta.nullable,
                        isRequired=is_req,
                        isIdentity=is_ident,
                        hasDefault=has_def,
                        requirementType=req_type,
                        validationStatus=val_status,
                        validationMessage=val_msg
                    ))

        # 9. Planned Operations Summary
        total_planned_ins = sum(planned_inserts_by_table.values())
        total_planned_skips = sum(planned_skips_by_table.values())

        planned_ops = PlannedOperationsSummary(
            totalPlannedInserts=total_planned_ins,
            totalPlannedUpdates=0,
            totalPlannedSkips=total_planned_skips,
            plannedInsertsByTable=planned_inserts_by_table,
            plannedUpdatesByTable={},
            plannedSkipsByTable=planned_skips_by_table,
            existingRowsSkipped=existing_rows_skipped,
            duplicateSourceRows=duplicate_source_rows,
            ambiguousRows=len(ambiguous_key_errors),
            matchingKeysUnavailable=sorted(matching_keys_unavailable),
            statement="This preview has not written any data to the database.",
            canExecute=validation_summary.can_execute
        )

        t_end = time.time()
        logger.info(f"[STAGE] SQL_PREVIEW_COMPLETED | rows={len(preview_rows)} | {t_end - t_prev_start:.3f}s")
        logger.info(f"[STAGE] DRY_RUN_COMPLETED | total_time={t_end - t0:.3f}s | zero writes performed")

        return PreviewResultTuple(
            preview_rows=preview_rows,
            dependency_plan=dependency_plan,
            planned_ops=planned_ops,
            validation_summary=validation_summary,
            active_tables_ordered=active_tables_ordered,
            value_resolutions=value_resolutions,
            value_resolution_summary=value_resolution_summary,
            value_mappings=all_value_mappings
        )

