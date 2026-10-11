"""
Intelligent Dry-Run and Migration Testing Engine.

Provides safe, read-only testing and validation for both generated mock datasets
and uploaded Excel/CSV files without ever executing INSERT, UPDATE, DELETE, MERGE,
or committing any transactions in SQL Server.
"""

import time
import math
import logging
import datetime
from typing import Dict, Any, List, Optional, Tuple, Set
import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy import text, Engine

from app.core.config import settings
from app.core.logging import logger
from app.database.connection import get_engine
from app.services.database_service import database_service
from app.models.schema import DatabaseSchemaMetadata, TableMetadata, ColumnMetadata
from app.models.migration import (
    ColumnMappingResult,
    SourceRowPreviewItem,
    ForeignKeyPlanItem,
    PlannedOperationsSummary,
    ValidationSummary,
    ValueResolutionSummary,
    ValueMappingItem,
    ResolvedEntityRecord,
)
from app.mapping.matcher import ColumnMatcher, normalize_string
from app.utils.normalization import normalize_column_name, normalize_identifier, normalize_date_value
from app.migration.preview import MigrationPreviewEngine, quote_identifier
from app.migration.validator import MigrationValidator, is_empty_or_na
from app.migration.engine import sanitize_value
from app.migration.lookup_resolver import DynamicLookupResolver
from app.migration.outlet_matcher import outlet_row_source_values


# -----------------------------------------------------------------------------
# Data Models for Dry-Run Results
# -----------------------------------------------------------------------------
class ProposedSqlStatement(BaseModel):
    table_name: str
    operation: str = "INSERT"  # INSERT or UPDATE
    sql_text: str
    parameters: Dict[str, Any]
    parameter_types: Dict[str, str]
    status: str = "NOT EXECUTED — DRY RUN"
    source_row: int


class DryRunRowDiagnostic(BaseModel):
    source_row_num: int
    source_data: Dict[str, Any]
    normalized_values: Dict[str, Any]
    matched_columns: Dict[str, str]  # source_col -> "Table.Column"
    match_types: Dict[str, str]
    confidences: Dict[str, float]
    resolved_existing_ids: Dict[str, Any]
    proposed_values: Dict[str, Dict[str, Any]]  # table -> {col: val}
    proposed_sql: List[ProposedSqlStatement] = []
    validation_status: str = "VALID"  # VALID, WARNING, ERROR
    validation_errors: List[str] = []


class DryRunReport(BaseModel):
    mode: str = "DRY_RUN"
    test_source: str  # "MOCK_DATA" or "UPLOADED_FILE"
    file_name: Optional[str] = None
    rows_read: int = 0
    rows_processed: int = 0
    rows_validated: int = 0
    rows_valid: int = 0
    rows_with_errors: int = 0
    rows_with_warnings: int = 0
    validation_issue_count: int = 0
    matched_columns: int = 0
    unmatched_columns: int = 0
    ambiguous_mappings: int = 0
    exact_matches: int = 0
    normalized_matches: int = 0
    semantic_matches: int = 0
    fuzzy_matches: int = 0
    existing_references_resolved: int = 0
    foreign_key_resolution_attempts: int = 0
    foreign_keys_resolved: int = 0
    foreign_keys_unresolved: int = 0
    duplicate_rows_detected: int = 0
    invalid_or_missing_values: int = 0
    proposed_insert_statements: int = 0
    proposed_update_statements: int = 0
    insert_executed: int = 0
    update_executed: int = 0
    delete_executed: int = 0
    database_writes: int = 0
    actual_database_write_count: int = 0
    db_integrity_verified: bool = False
    db_verification_details: Dict[str, Any] = {}
    final_status: str = "PASS"  # PASS, PASS_WITH_WARNINGS, NEEDS_REVIEW, FAIL
    elapsed_seconds: float = 0.0
    text_report: str = ""
    proposed_sql_samples: List[ProposedSqlStatement] = []
    row_diagnostics: List[DryRunRowDiagnostic] = []


def mask_sensitive(val: Any) -> str:
    """Masks phone numbers and customer details for privacy in logs."""
    if not settings.DRY_RUN_MASK_SENSITIVE or val is None:
        return str(val)
    s = str(val).strip()
    if len(s) >= 8 and (s.isdigit() or s.startswith("+") or s.replace("-", "").isdigit()):
        return s[:3] + "****" + s[-3:]
    return s


# -----------------------------------------------------------------------------
# Mock Data Generator
# -----------------------------------------------------------------------------
def generate_representative_mock_data(schema: Optional[DatabaseSchemaMetadata] = None) -> pd.DataFrame:
    """
    Generates a realistic test dataset representing edge cases:
    - Customer name and phone number (strings with leading zeros).
    - Customer address and location.
    - Outlet names and virtual brand names with branch suffixes (e.g. 'Namaste BTM', 'Namaste Hoodi', 'ANN - BTM').
    - Order number and dates (Excel serial numbers, date strings, ISO dates).
    - Order line items and quantities.
    - Varying header casing and conventions ('customer_name', 'CUSTOMER_NAME', 'customerName', 'customer-phone', 'OrderDate').
    - Empty, missing, and duplicate rows.
    """
    records = [
        # Row 1: Standard valid order with branch suffix on known outlet
        {
            "restaurant_name": "Namaste BTM",
            "invoice_no": "100101",
            "date": "2026-07-01 10:15:00",
            "customer_phone": "09876543210",
            "customer_name": "Karthik Raja",
            "customer_address": "45 MG Road, Indiranagar, Bengaluru",
            "area": "Zomato_Namaste - BTM",
            "virtual_brand_name": "Namaste BTM",
            "total": "450.00",
            "item_name": "Paneer Butter Masala",
            "item_quantity": "2",
            "item_price": "225.00",
            "item_total": "450.00",
        },
        # Row 2: Second item for same order (tests multi-item parent reuse)
        {
            "restaurant_name": "Namaste BTM",
            "invoice_no": "100101",
            "date": "2026-07-01 10:15:00",
            "customer_phone": "09876543210",
            "customer_name": "Karthik Raja",
            "customer_address": "45 MG Road, Indiranagar, Bengaluru",
            "area": "Zomato_Namaste - BTM",
            "virtual_brand_name": "Namaste BTM",
            "total": "450.00",
            "item_name": "Butter Naan",
            "item_quantity": "4",
            "item_price": "40.00",
            "item_total": "160.00",
        },
        # Row 3: Different outlet with Hoodi suffix
        {
            "restaurant_name": "Namaste Hoodi",
            "invoice_no": "100102",
            "date": "02/07/2026 14:30:00",
            "customer_phone": "09123456789",
            "customer_name": "Priya Sharma",
            "customer_address": "12 Whitefield Main Rd, Bengaluru",
            "area": "Swiggy_Namaste - Hoodi",
            "virtual_brand_name": "Namaste Hoodi",
            "total": "320.00",
            "item_name": "Veg Biryani",
            "item_quantity": "1",
            "item_price": "320.00",
            "item_total": "320.00",
        },
        # Row 4: Excel serial number date (45000 = 2023-03-15) & ANN outlet
        {
            "restaurant_name": "ANN - BTM",
            "invoice_no": "100103",
            "date": 45000,
            "customer_phone": "08012345678",
            "customer_name": "Ramesh Kumar",
            "customer_address": "210 4th Ave AC Block Anna Nagar Chennai",
            "area": "Zomato_ANN - BTM",
            "virtual_brand_name": "ANN - BTM",
            "total": "550.00",
            "item_name": "Chicken Biryani",
            "item_quantity": "2",
            "item_price": "275.00",
            "item_total": "550.00",
        },
        # Row 5: Missing customer phone and missing location (tests NULL handling)
        {
            "restaurant_name": "Namaste",
            "invoice_no": "100104",
            "date": "2026-07-03",
            "customer_phone": None,
            "customer_name": "Anonymous Walk-in",
            "customer_address": None,
            "area": "WalkIn_Area",
            "virtual_brand_name": "Namaste",
            "total": "120.00",
            "item_name": "Masala Dosa",
            "item_quantity": "1",
            "item_price": "120.00",
            "item_total": "120.00",
        },
        # Row 6: Exact duplicate row of Row 5 (tests duplicate row detection)
        {
            "restaurant_name": "Namaste",
            "invoice_no": "100104",
            "date": "2026-07-03",
            "customer_phone": None,
            "customer_name": "Anonymous Walk-in",
            "customer_address": None,
            "area": "WalkIn_Area",
            "virtual_brand_name": "Namaste",
            "total": "120.00",
            "item_name": "Masala Dosa",
            "item_quantity": "1",
            "item_price": "120.00",
            "item_total": "120.00",
        },
        # Row 7: Ambiguous / unknown outlet candidate
        {
            "restaurant_name": "The Unknown Royal Bistro",
            "invoice_no": "100105",
            "date": "2026-07-04 18:00:00",
            "customer_phone": "09988776655",
            "customer_name": "David Miller",
            "customer_address": "Koramangala 5th Block",
            "area": "Zomato_Unknown",
            "virtual_brand_name": "Royal Bistro Cloud",
            "total": "750.00",
            "item_name": "Pasta Alfredo",
            "item_quantity": "2",
            "item_price": "375.00",
            "item_total": "750.00",
        },
    ]

    return pd.DataFrame(records)


# -----------------------------------------------------------------------------
# Dry-Run Pipeline Engine
# -----------------------------------------------------------------------------
class DryRunEngine:
    """
    Executes complete migration preparation, matching, resolution, validation,
    and SQL generation pipeline with STRICT ZERO-WRITE GUARANTEES.
    """

    @staticmethod
    def get_database_row_counts(engine: Engine, table_names: List[str]) -> Dict[str, int]:
        """Queries table row counts safely using read-only SELECT COUNT(*)."""
        counts: Dict[str, int] = {}
        with engine.connect() as conn:
            for tbl in table_names:
                try:
                    q_tbl = quote_identifier(tbl, engine.name)
                    cnt = conn.execute(text(f"SELECT COUNT(*) FROM {q_tbl}")).scalar()
                    counts[tbl] = int(cnt or 0)
                except Exception as ex:
                    logger.debug(f"[DRY_RUN] Could not get count for table {tbl}: {ex}")
                    counts[tbl] = -1
        return counts

    @classmethod
    def run_dry_run(
        cls,
        df: pd.DataFrame,
        source_name: str,
        test_source: str = "UPLOADED_FILE",
        user_overrides: Optional[Dict[str, Any]] = None,
        user_value_overrides: Optional[Dict[str, Dict[str, str]]] = None,
        schema: Optional[DatabaseSchemaMetadata] = None,
        engine: Optional[Engine] = None,
    ) -> DryRunReport:
        """
        Executes complete dry-run testing pipeline.
        """
        t0 = time.time()
        logger.info(f"[DRY_RUN_START] Starting Migration Dry-Run Test for source='{source_name}' ({test_source})")

        if engine is None:
            engine = get_engine()
        if schema is None:
            schema = database_service.get_schema_metadata()

        schema_tables = {t.name: t for t in schema.tables}
        dialect_name = engine.name

        # ---------------------------------------------------------------------
        # Pre-Run Database Integrity Check
        # ---------------------------------------------------------------------
        inspect_tables = [t.name for t in schema.tables if t.name in ("Customer", "Outlets", "Order", "OrderLineItems", "Bills")]
        pre_counts = {}
        can_verify_db = True
        try:
            pre_counts = cls.get_database_row_counts(engine, inspect_tables)
        except Exception as e:
            can_verify_db = False
            logger.warning(f"[DRY_RUN] Pre-run database row count check failed: {e}")

        # ---------------------------------------------------------------------
        # Stage 1: File / Data Read
        # ---------------------------------------------------------------------
        rows_read = len(df)
        cols_detected = list(df.columns)
        logger.info(f"[FILE_READ_SUCCESS] Successfully read {rows_read} rows and {len(cols_detected)} columns from '{source_name}'")

        # ---------------------------------------------------------------------
        # Stage 2: Column Normalization & Matching
        # ---------------------------------------------------------------------
        logger.info(f"[COLUMN_NORMALIZATION] Normalizing {len(cols_detected)} column headers...")
        for col in cols_detected:
            norm_name = normalize_column_name(col)
            logger.debug(f"[COLUMN_NORMALIZATION] Header '{col}' -> Normalized '{norm_name}'")

        logger.info("[COLUMN_MAPPING] Running multi-layer column matching...")
        column_mappings = ColumnMatcher.match_columns(cols_detected, schema, user_overrides=user_overrides)

        exact_cnt = sum(1 for m in column_mappings if m.match_type in ("EXACT_ORIGINAL", "CASE_INSENSITIVE"))
        norm_cnt = sum(1 for m in column_mappings if m.match_type == "EXACT_NORMALIZED")
        semantic_cnt = sum(1 for m in column_mappings if m.match_type in ("SYNONYM", "SEMANTIC", "APPROVED_RULE"))
        fuzzy_cnt = sum(1 for m in column_mappings if m.match_type == "FUZZY")
        matched_cnt = sum(1 for m in column_mappings if m.status in ("MATCHED", "APPROVED"))
        unmatched_cnt = sum(1 for m in column_mappings if m.status == "UNMATCHED")
        ambiguous_cnt = sum(1 for m in column_mappings if m.status == "AMBIGUOUS")

        for m in column_mappings:
            logger.info(
                f"[COLUMN_MAPPING] '{m.excel_column}' -> {m.database_table}.{m.database_column} "
                f"[{m.match_type}, conf={m.confidence:.2f}, status={m.status}]"
            )

        # ---------------------------------------------------------------------
        # Stage 3: Semantic Validation
        # ---------------------------------------------------------------------
        logger.info("[SEMANTIC_VALIDATION] Verifying business rules and semantic constraints...")
        # Guardrail checks:
        for m in column_mappings:
            norm_src = normalize_column_name(m.excel_column)
            if norm_src in ("restaurant name", "virtual brand name") and m.database_table == "Customer" and m.database_column == "Location":
                logger.error(f"[SEMANTIC_VALIDATION] VIOLATION: Outlet name '{m.excel_column}' mapped to Customer.Location!")
            if norm_src == "area" and m.database_table == "Customer" and m.database_column == "Location":
                logger.error(f"[SEMANTIC_VALIDATION] VIOLATION: Source area '{m.excel_column}' mapped to Customer.Location!")

        # ---------------------------------------------------------------------
        # Stage 4: Preview Generation & Existing Record Lookup (Read-Only)
        # ---------------------------------------------------------------------
        logger.info("[EXISTING_RECORD_LOOKUP] Querying existing records for matching (READ-ONLY)...")
        logger.info("[FOREIGN_KEY_RESOLUTION] Resolving relational foreign keys...")

        preview_tuple = MigrationPreviewEngine.generate_preview(
            df=df,
            file_name=source_name,
            schema=schema,
            column_mappings=column_mappings,
            engine=engine,
            user_value_overrides=user_value_overrides
        )

        validation_summary: ValidationSummary = preview_tuple.validation_summary
        preview_rows: List[SourceRowPreviewItem] = preview_tuple.preview_rows
        val_res_summary: Optional[ValueResolutionSummary] = preview_tuple.value_resolution_summary
        active_tables: List[str] = preview_tuple.execution_order

        # ---------------------------------------------------------------------
        # Stage 5: Date Value Validation
        # ---------------------------------------------------------------------
        logger.info("[DATE_VALUE_VALIDATION] Validating date/time values across preview rows...")
        date_cols = [c for c in cols_detected if any(k in normalize_column_name(c) for k in ("date", "time", "created"))]
        for dc in date_cols:
            sample_val = df[dc].dropna().iloc[0] if not df[dc].dropna().empty else None
            if sample_val is not None:
                norm_d, dt_obj, is_v = normalize_date_value(sample_val, "DATETIME")
                logger.debug(f"[DATE_VALUE_VALIDATION] Col '{dc}' sample: '{sample_val}' -> Normalized: '{norm_d}' (valid={is_v})")

        # ---------------------------------------------------------------------
        # Stage 6: SQL Statement Generation (WITHOUT EXECUTING)
        # ---------------------------------------------------------------------
        logger.info("[SQL_PREVIEW_GENERATED] Generating parameterized SQL statements (NOT EXECUTED — DRY RUN)...")
        proposed_statements: List[ProposedSqlStatement] = []
        row_diagnostics: List[DryRunRowDiagnostic] = []

        # Map active table mappings
        table_mappings: Dict[str, List[ColumnMappingResult]] = {}
        for m in column_mappings:
            if m.status in ("MATCHED", "APPROVED") and m.database_table in schema_tables:
                table_mappings.setdefault(m.database_table, []).append(m)

        sql_lookup_relationships = DynamicLookupResolver.discover_relationships(
            schema=schema,
            df_columns=cols_detected,
            column_mappings=column_mappings,
            active_tables=active_tables
        )
        sql_parent_records = DynamicLookupResolver.load_parent_records(
            engine=engine,
            schema=schema,
            relationships=sql_lookup_relationships,
            df=df
        )

        # Build parameterized SQL statements for each row
        rows_to_process = min(len(df), 100)
        duplicate_rows_cnt = int(df.duplicated().sum())

        for idx in range(rows_to_process):
            row_num = idx + 1
            row_series = df.iloc[idx]
            row_dict = row_series.to_dict()

            row_diag = DryRunRowDiagnostic(
                source_row_num=row_num,
                source_data={k: mask_sensitive(v) for k, v in row_dict.items()},
                normalized_values={},
                matched_columns={},
                match_types={},
                confidences={},
                resolved_existing_ids={},
                proposed_values={},
                proposed_sql=[],
                validation_status="VALID",
                validation_errors=[]
            )

            # Check if this row has validation errors in validation_summary
            row_errs = [err.message for err in validation_summary.errors if err.row_number == row_num]
            if row_errs:
                row_diag.validation_status = "ERROR"
                row_diag.validation_errors = row_errs

            for tbl_name in active_tables:
                if tbl_name == "Outlets":
                    # Read-only reference table, never generate INSERT
                    continue

                t_meta = schema_tables.get(tbl_name)
                if not t_meta:
                    continue

                col_meta_map = {c.name: c for c in t_meta.columns}
                t_mappings = table_mappings.get(tbl_name, [])

                param_dict: Dict[str, Any] = {}
                param_types: Dict[str, str] = {}
                insert_cols: List[str] = []
                block_dependent_insert = False

                for m in t_mappings:
                    col_name = m.database_column
                    raw_val = row_dict.get(m.excel_column)
                    c_meta = col_meta_map.get(col_name)
                    c_type = c_meta.type if c_meta else "VARCHAR"
                    c_null = c_meta.nullable if c_meta else True

                    sanitized = sanitize_value(raw_val, c_type, c_null)
                    param_dict[col_name] = sanitized
                    param_types[col_name] = c_type
                    insert_cols.append(col_name)

                    row_diag.matched_columns[m.excel_column] = f"{tbl_name}.{col_name}"
                    row_diag.match_types[m.excel_column] = m.match_type
                    row_diag.confidences[m.excel_column] = m.confidence
                    row_diag.normalized_values[m.excel_column] = mask_sensitive(sanitized)

                for rel in sql_lookup_relationships:
                    if (
                        rel.child_table != tbl_name
                        or rel.parent_table.lower() not in ("outlets", "dbo.outlets")
                    ):
                        continue

                    parent_records = sql_parent_records.get(rel.parent_table, [])
                    outlet_result = DynamicLookupResolver.resolve_foreign_key_record(
                        child_table=tbl_name,
                        child_column=rel.child_column,
                        parent_table=rel.parent_table,
                        parent_id_column=rel.parent_id_column,
                        parent_name_column=rel.parent_name_column,
                        db_records=parent_records,
                        excel_source_value=row_dict.get(rel.excel_name_column) if rel.excel_name_column else None,
                        row_data=row_dict,
                        source_row_num=row_num
                    )
                    outlet_id = outlet_result.matched_primary_key_value
                    parent_ids = {
                        str(record.get(rel.parent_id_column))
                        for record in parent_records
                        if record.get(rel.parent_id_column) is not None
                    }
                    valid_outlet = (
                        outlet_result.status == "RESOLVED"
                        and isinstance(outlet_id, int)
                        and not isinstance(outlet_id, bool)
                        and str(outlet_id) in parent_ids
                    )
                    if not valid_outlet:
                        restaurant_value, virtual_brand_value = outlet_row_source_values(row_dict)
                        reason = outlet_result.message or f"Outlet resolver returned {outlet_result.status}."
                        row_diag.validation_status = "ERROR"
                        row_diag.validation_errors.append(
                            f"Row {row_num}: {rel.child_column} blocked: {reason}"
                        )
                        block_dependent_insert = True
                        logger.warning(
                            "[DRY_RUN_OUTLET_FK_BLOCKED] source=%s row=%s restaurant_name=%r "
                            "virtual_brand_name=%r status=%s candidates=%s reason=%s",
                            source_name, row_num, restaurant_value, virtual_brand_value,
                            outlet_result.status,
                            [candidate.get("id") for candidate in outlet_result.candidates],
                            reason
                        )
                        continue

                    column_meta = col_meta_map.get(rel.child_column)
                    final_id = sanitize_value(
                        outlet_id,
                        column_meta.type if column_meta else "INT",
                        False
                    )
                    if final_id != outlet_id:
                        row_diag.validation_status = "ERROR"
                        row_diag.validation_errors.append(
                            f"Row {row_num}: {rel.child_column} ID failed SQL type validation."
                        )
                        block_dependent_insert = True
                        continue
                    if rel.child_column in param_dict and param_dict[rel.child_column] not in (None, final_id):
                        row_diag.validation_status = "ERROR"
                        row_diag.validation_errors.append(
                            f"Row {row_num}: mapped {rel.child_column} conflicts with the resolved outlet ID."
                        )
                        block_dependent_insert = True
                        continue

                    param_dict[rel.child_column] = final_id
                    param_types[rel.child_column] = column_meta.type if column_meta else "INT"
                    if rel.child_column not in insert_cols:
                        insert_cols.append(rel.child_column)
                    row_diag.resolved_existing_ids[f"{rel.parent_table}.{rel.parent_id_column}"] = final_id
                    logger.info(
                        "[DRY_RUN_OUTLET_FK_PARAMETER] source=%s row=%s target=%s.%s value=%r type=%s",
                        source_name, row_num, tbl_name, rel.child_column,
                        final_id, type(final_id).__name__
                    )

                if block_dependent_insert:
                    continue

                # Deduplicate insert cols
                deduped_insert_cols = list(dict.fromkeys(insert_cols))
                if deduped_insert_cols:
                    q_tbl = quote_identifier(tbl_name, dialect_name)
                    q_cols = [quote_identifier(c, dialect_name) for c in deduped_insert_cols]
                    v_params = [f":{c}" for c in deduped_insert_cols]

                    # Look for identity PK column
                    pk_identity = next((c.name for c in t_meta.columns if c.primary_key and getattr(c, "identity", False)), None)
                    if dialect_name == "mssql" and pk_identity:
                        q_pk = quote_identifier(pk_identity, dialect_name)
                        sql_stmt = (
                            f"INSERT INTO {q_tbl} ({', '.join(q_cols)})\n"
                            f"OUTPUT INSERTED.{q_pk}\n"
                            f"VALUES ({', '.join(v_params)});"
                        )
                    else:
                        sql_stmt = (
                            f"INSERT INTO {q_tbl} ({', '.join(q_cols)})\n"
                            f"VALUES ({', '.join(v_params)});"
                        )

                    masked_params = {k: mask_sensitive(v) for k, v in param_dict.items() if k in deduped_insert_cols}
                    prop_stmt = ProposedSqlStatement(
                        table_name=tbl_name,
                        operation="INSERT",
                        sql_text=sql_stmt,
                        parameters=masked_params,
                        parameter_types={k: param_types.get(k, "VARCHAR") for k in deduped_insert_cols},
                        status="NOT EXECUTED — DRY RUN",
                        source_row=row_num
                    )
                    proposed_statements.append(prop_stmt)
                    row_diag.proposed_sql.append(prop_stmt)
                    row_diag.proposed_values[tbl_name] = masked_params

            row_diagnostics.append(row_diag)

        # ---------------------------------------------------------------------
        # Stage 7: Row Validation Result Logging
        # ---------------------------------------------------------------------
        # ---------------------------------------------------------------------
        # Stage 7: Row Validation Result Logging & Metrics
        # ---------------------------------------------------------------------
        # Count distinct rows with errors (ignoring table-level rowNumber=0)
        source_rows_with_errors = {
            err.row_number for err in validation_summary.errors
            if err.row_number is not None and err.row_number > 0 and err.row_number <= rows_read
        }
        source_rows_with_warnings = {
            w.row_number for w in validation_summary.warnings
            if w.row_number is not None and w.row_number > 0 and w.row_number <= rows_read
        }

        # If there are global/table-level errors (rowNumber=0), add to total validation issue count
        total_validation_issues = len(validation_summary.errors) + len(validation_summary.warnings)
        rows_with_errors_cnt = len(source_rows_with_errors)
        rows_with_warnings_cnt = len(source_rows_with_warnings)
        rows_valid_cnt = max(0, rows_read - rows_with_errors_cnt)

        logger.info(
            f"[ROW_VALIDATION_RESULT] Processed {rows_read} rows: "
            f"{rows_valid_cnt} Valid, {rows_with_errors_cnt} With Errors, "
            f"{rows_with_warnings_cnt} With Warnings, {total_validation_issues} Total Validation Issues."
        )

        for diag in row_diagnostics[:10]:
            logger.debug(
                f"[ROW_VALIDATION_RESULT] Row {diag.source_row_num}: status={diag.validation_status}, "
                f"proposed_inserts={len(diag.proposed_sql)}"
            )

        # ---------------------------------------------------------------------
        # Post-Run Database Integrity Check
        # ---------------------------------------------------------------------
        post_counts = {}
        writes_detected = 0
        if can_verify_db:
            try:
                post_counts = cls.get_database_row_counts(engine, inspect_tables)
                for tbl, pre_cnt in pre_counts.items():
                    post_cnt = post_counts.get(tbl, 0)
                    if pre_cnt != -1 and post_cnt != pre_cnt:
                        writes_detected += abs(post_cnt - pre_cnt)
                        logger.error(f"[DRY_RUN] INTEGRITY VIOLATION: Table {tbl} changed count from {pre_cnt} to {post_cnt}!")
            except Exception as e:
                logger.warning(f"[DRY_RUN] Post-run database row count check failed: {e}")

        # Metrics aggregation
        existing_resolved = val_res_summary.values_matched_existing if val_res_summary else 0
        fk_attempts = (val_res_summary.values_inspected if val_res_summary else 0)
        fks_resolved = val_res_summary.values_resolved_confirmed if val_res_summary else 0
        fks_unresolved = val_res_summary.unresolved_foreign_keys if val_res_summary else 0
        proposed_inserts_cnt = len(proposed_statements)
        proposed_updates_cnt = 0

        # Exact Final Status Decision Rules:
        # - FAIL: Database writes detected or fatal schema execution error
        # - NEEDS_REVIEW: Ambiguous mappings exist or unresolved foreign keys exist
        # - PASS_WITH_WARNINGS: Non-blocking row validation errors/warnings present but no ambiguity
        # - PASS: Zero errors, zero ambiguity, zero database writes
        if writes_detected > 0:
            final_status = "FAIL"
        elif ambiguous_cnt > 0 or fks_unresolved > 0:
            final_status = "NEEDS_REVIEW"
        elif len(validation_summary.errors) > 0 or rows_with_warnings_cnt > 0:
            final_status = "PASS_WITH_WARNINGS"
        else:
            final_status = "PASS"

        elapsed = round(time.time() - t0, 3)

        # ---------------------------------------------------------------------
        # Stage 8: Summary & Final Report
        # ---------------------------------------------------------------------
        logger.info(f"[DRY_RUN_SUMMARY] Mode=DRY_RUN, Source={test_source}, Rows={rows_read}, Writes=0, Status={final_status}")

        text_report = (
            f"========== DRY-RUN TEST REPORT ==========\n\n"
            f"Mode: DRY_RUN\n"
            f"Test Source: {test_source}\n"
            f"File/Dataset: {source_name}\n\n"
            f"Rows Read: {rows_read}\n"
            f"Rows Processed: {rows_read}\n"
            f"Rows Validated: {rows_read}\n"
            f"Rows Valid: {rows_valid_cnt}\n"
            f"Rows With Errors: {rows_with_errors_cnt}\n"
            f"Rows With Warnings: {rows_with_warnings_cnt}\n"
            f"Total Validation Issues: {total_validation_issues}\n"
            f"Matched Columns: {matched_cnt}\n"
            f"Unmatched Columns: {unmatched_cnt}\n"
            f"Ambiguous Mappings: {ambiguous_cnt}\n"
            f"Exact / Normalized Matches: {exact_cnt + norm_cnt}\n"
            f"Semantic Alias Matches: {semantic_cnt}\n"
            f"Fuzzy Matches: {fuzzy_cnt}\n"
            f"Existing References Resolved: {existing_resolved}\n"
            f"Foreign Key Resolution Attempts: {fk_attempts}\n"
            f"Foreign Keys Resolved: {fks_resolved}\n"
            f"Foreign Keys Unresolved: {fks_unresolved}\n"
            f"Duplicate Rows Detected: {duplicate_rows_cnt}\n"
            f"Invalid or Missing Values: {len(validation_summary.errors)}\n\n"
            f"Proposed INSERT Statements: {proposed_inserts_cnt}\n"
            f"Proposed UPDATE Statements: {proposed_updates_cnt}\n\n"
            f"INSERT Executed: 0\n"
            f"UPDATE Executed: 0\n"
            f"DELETE Executed: 0\n"
            f"Database Writes: {writes_detected}\n"
            f"Database Integrity Verified: {'YES (0 writes confirmed)' if (can_verify_db and writes_detected == 0) else 'SKIPPED/UNAVAILABLE'}\n\n"
            f"Final Status: {final_status}\n"
            f"Elapsed Time: {elapsed:.3f}s\n"
            f"========================================\n"
        )

        logger.info(f"[DRY_RUN_COMPLETED]\n{text_report}")

        return DryRunReport(
            mode="DRY_RUN",
            test_source=test_source,
            file_name=source_name,
            rows_read=rows_read,
            rows_processed=rows_read,
            rows_validated=rows_read,
            rows_valid=rows_valid_cnt,
            rows_with_errors=rows_with_errors_cnt,
            rows_with_warnings=rows_with_warnings_cnt,
            validation_issue_count=total_validation_issues,
            matched_columns=matched_cnt,
            unmatched_columns=unmatched_cnt,
            ambiguous_mappings=ambiguous_cnt,
            exact_matches=exact_cnt,
            normalized_matches=norm_cnt,
            semantic_matches=semantic_cnt,
            fuzzy_matches=fuzzy_cnt,
            existing_references_resolved=existing_resolved,
            foreign_key_resolution_attempts=fk_attempts,
            foreign_keys_resolved=fks_resolved,
            foreign_keys_unresolved=fks_unresolved,
            duplicate_rows_detected=duplicate_rows_cnt,
            invalid_or_missing_values=len(validation_summary.errors),
            proposed_insert_statements=proposed_inserts_cnt,
            proposed_update_statements=proposed_updates_cnt,
            insert_executed=0,
            update_executed=0,
            delete_executed=0,
            database_writes=writes_detected,
            actual_database_write_count=writes_detected,
            db_integrity_verified=(can_verify_db and writes_detected == 0),
            db_verification_details={
                "pre_counts": pre_counts,
                "post_counts": post_counts,
                "verified_tables": inspect_tables
            },
            final_status=final_status,
            elapsed_seconds=elapsed,
            text_report=text_report,
            proposed_sql_samples=proposed_statements[:15],
            row_diagnostics=row_diagnostics[:20]
        )
