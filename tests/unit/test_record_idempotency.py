from unittest.mock import patch
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, event, text

from app.migration.engine import MigrationEngine
from app.models.migration import ColumnMappingResult
from app.models.schema import ColumnMetadata, DatabaseSchemaMetadata, TableMetadata, UniqueKeyInfo


def _vendor_schema():
    vendor = TableMetadata(
        name="Vendors",
        schema_name="dbo",
        columns=[
            ColumnMetadata(name="Vendor_ID", type="INT", nullable=False, primary_key=True, identity=True),
            ColumnMetadata(name="Vendor_Name", type="VARCHAR", nullable=False, unique=True)
        ],
        primary_keys=["Vendor_ID"],
        unique_keys=[UniqueKeyInfo(name="UQ_Vendors_Vendor_Name", columns=["Vendor_Name"])]
    )
    return DatabaseSchemaMetadata(database_name="TestDB", db_type="sqlite", tables=[vendor])


def _run_upload(engine, frame, upload_id, schema=None):
    mappings = [
        ColumnMappingResult(
            excelColumn="Supplier",
            databaseTable="Vendors",
            databaseColumn="Vendor_Name",
            status="MATCHED"
        )
    ]
    with patch("app.services.excel_service.excel_service.get_upload_session", return_value={
        "filePath": "memory.csv", "sheetName": None, "headerRowNumber": 1
    }), patch("app.migration.engine.get_engine", return_value=engine), patch(
        "app.services.database_service.database_service.get_schema_metadata",
        return_value=schema or _vendor_schema()
    ), patch("app.migration.engine.load_file_dataframe", return_value=frame), patch(
        "app.migration.engine.pd.io.common.file_exists", return_value=True
    ), patch("app.mapping.matcher.ColumnMatcher.match_columns", return_value=mappings):
        return MigrationEngine.execute_migration(upload_id)


def test_repeated_upload_reuses_existing_record_by_declared_unique_key():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE Vendors (Vendor_ID INTEGER PRIMARY KEY AUTOINCREMENT, Vendor_Name TEXT NOT NULL UNIQUE)"
        ))
    frame = pd.DataFrame([{"Supplier": "Northwind Foods"}])

    first = _run_upload(engine, frame, "vendor_upload_first")
    second = _run_upload(engine, frame, "vendor_upload_repeat")

    assert first.total_inserted == 1
    assert second.total_inserted == 0
    assert second.total_reused == 1
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM Vendors")).scalar_one() == 1


def test_mixed_existing_and_new_records_only_inserts_new_key():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE Vendors (Vendor_ID INTEGER PRIMARY KEY AUTOINCREMENT, Vendor_Name TEXT NOT NULL UNIQUE)"
        ))
    _run_upload(engine, pd.DataFrame([{"Supplier": "Northwind Foods"}]), "vendor_upload_seed")

    result = _run_upload(
        engine,
        pd.DataFrame([{"Supplier": "Northwind Foods"}, {"Supplier": "New Coast Produce"}]),
        "vendor_upload_mixed"
    )

    assert result.total_inserted == 1
    assert result.total_reused == 1
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM Vendors")).scalar_one() == 2


def test_duplicate_source_key_keeps_first_and_reports_duplicate():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE Vendors (Vendor_ID INTEGER PRIMARY KEY AUTOINCREMENT, Vendor_Name TEXT NOT NULL UNIQUE)"
        ))
    frame = pd.DataFrame([
        {"Supplier": "Northwind Foods"},
        {"Supplier": "Northwind Foods"},
        {"Supplier": "Northwind Foods"}
    ])

    result = _run_upload(engine, frame, "vendor_upload_source_duplicates")

    assert result.total_inserted == 1
    assert result.duplicate_source_rows == 2
    assert result.total_skipped == 2
    assert sum(outcome["status"] == "DUPLICATE_SOURCE" for outcome in result.record_outcomes) == 2
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM Vendors")).scalar_one() == 1


def test_name_without_unique_constraint_is_not_guessed_as_matching_key():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE Vendors (Vendor_ID INTEGER PRIMARY KEY AUTOINCREMENT, Vendor_Name TEXT NOT NULL)"
        ))
        connection.execute(text("INSERT INTO Vendors (Vendor_Name) VALUES ('Northwind Foods')"))

    schema = _vendor_schema()
    schema.tables[0].columns[1].unique = False
    schema.tables[0].unique_keys = []
    result = _run_upload(engine, pd.DataFrame([{"Supplier": "Northwind Foods"}]), "vendor_no_key", schema)

    assert result.total_reused == 0
    assert result.total_inserted == 1
    assert result.matching_keys_unavailable == ["Vendors"]
    assert any(w.error_type == "SAFE_MATCHING_UNAVAILABLE" for w in result.warnings)


def test_text_unique_key_preserves_leading_zeroes():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE Vendors (Vendor_ID INTEGER PRIMARY KEY AUTOINCREMENT, Vendor_Name TEXT NOT NULL UNIQUE)"
        ))

    first = _run_upload(engine, pd.DataFrame([{"Supplier": "00123"}]), "vendor_zero_first")
    second = _run_upload(engine, pd.DataFrame([{"Supplier": "123"}]), "vendor_zero_second")

    assert first.total_inserted == 1
    assert second.total_inserted == 1
    with engine.connect() as connection:
        values = connection.execute(text("SELECT Vendor_Name FROM Vendors ORDER BY Vendor_Name")).scalars().all()
        assert values == ["00123", "123"]


def test_case_insensitive_collation_deduplicates_case_variants():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE Vendors (Vendor_ID INTEGER PRIMARY KEY AUTOINCREMENT, Vendor_Name TEXT NOT NULL UNIQUE)"
        ))
    schema = _vendor_schema()
    schema.tables[0].columns[1].collation = "SQL_Latin1_General_CP1_CI_AS"
    frame = pd.DataFrame([{"Supplier": "Northwind Foods"}, {"Supplier": "northwind foods"}])

    result = _run_upload(engine, frame, "vendor_case_variant", schema)

    assert result.total_inserted == 1
    assert result.duplicate_source_rows == 1
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM Vendors")).scalar_one() == 1


def test_multiple_existing_rows_for_declared_key_fail_as_ambiguous():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE Vendors (Vendor_ID INTEGER PRIMARY KEY, Vendor_Name TEXT NOT NULL)"
        ))
        connection.execute(text(
            "INSERT INTO Vendors (Vendor_ID, Vendor_Name) VALUES (10, 'Northwind Foods'), (20, 'Northwind Foods')"
        ))

    with pytest.raises(RuntimeError, match="Ambiguous"):
        _run_upload(engine, pd.DataFrame([{"Supplier": "Northwind Foods"}]), "vendor_ambiguous_existing")

    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM Vendors")).scalar_one() == 2


def test_existing_key_lookups_are_batched_not_table_scans():
    from app.migration.record_matcher import load_existing_matches

    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE Vendors (Vendor_ID INTEGER PRIMARY KEY, Vendor_Name TEXT NOT NULL UNIQUE)"
        ))
        connection.execute(text(
            "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM seq WHERE x < 401) "
            "INSERT INTO Vendors (Vendor_ID, Vendor_Name) SELECT x, 'Vendor ' || x FROM seq"
        ))

    table = _vendor_schema().tables[0]
    statement_count = 0

    def count_selects(_conn, _cursor, statement, _parameters, _context, _many):
        nonlocal statement_count
        if statement.lstrip().upper().startswith("SELECT"):
            statement_count += 1

    event.listen(engine, "after_cursor_execute", count_selects)
    keys = [[f"Vendor {index}"] for index in range(1, 402)]
    with engine.connect() as connection:
        matches = load_existing_matches(
            connection=connection,
            engine=engine,
            table=table,
            key_columns=["Vendor_Name"],
            source_keys=keys,
            primary_key_columns=["Vendor_ID"],
            batch_size=200
        )

    assert len(matches) == 401
    assert statement_count == 3


def test_sql_server_candidate_lookup_uses_unique_key_range_locks():
    from app.migration.record_matcher import load_existing_matches

    class EmptyMappings:
        def mappings(self):
            return self

        def all(self):
            return []

    class CaptureConnection:
        def __init__(self):
            self.statements = []

        def execute(self, statement, parameters):
            self.statements.append((str(statement), parameters))
            return EmptyMappings()

    table = _vendor_schema().tables[0]
    connection = CaptureConnection()
    load_existing_matches(
        connection=connection,
        engine=SimpleNamespace(name="mssql"),
        table=table,
        key_columns=["Vendor_Name"],
        source_keys=[["Northwind Foods"]],
        primary_key_columns=["Vendor_ID"],
        lock_key_ranges=True,
        key_index_name="UQ_Vendors_Vendor_Name"
    )

    sql, params = connection.statements[0]
    assert "UPDLOCK" in sql
    assert "HOLDLOCK" in sql
    assert "INDEX([UQ_Vendors_Vendor_Name])" in sql
    assert params["k_0_0"] == "Northwind Foods"