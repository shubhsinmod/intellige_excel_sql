from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd
from sqlalchemy import Engine, text
from sqlalchemy.engine import Connection

from app.models.schema import ColumnMetadata, TableMetadata

CanonicalKey = Tuple[Tuple[str, str], ...]


def _quote_identifier(value: str, dialect: str) -> str:
    clean = str(value).replace("[", "").replace("]", "").replace('"', "").replace("`", "")
    if dialect == "mssql":
        return f"[{clean}]"
    if dialect in ("mysql", "mariadb"):
        return f"`{clean}`"
    return f'"{clean}"'


def quote_table(table: TableMetadata, dialect: str) -> str:
    if dialect == "sqlite":
        return _quote_identifier(table.name, dialect)
    return f"{_quote_identifier(table.schema_name, dialect)}.{_quote_identifier(table.name, dialect)}"


def mapped_matching_keys(
    table: TableMetadata,
    mapped_database_columns: Iterable[str]
) -> List[Tuple[str, ...]]:
    """Return source-backed primary/unique keys, strongest declared key first."""
    mapped = set(mapped_database_columns)
    candidates: List[Tuple[bool, Tuple[str, ...]]] = []

    for key in table.unique_keys:
        columns = tuple(key.columns)
        if columns and all(column in mapped for column in columns):
            candidates.append((key.primary_key, columns))

    pk_columns = tuple(table.primary_keys or [column.name for column in table.columns if column.primary_key])
    if pk_columns and all(column in mapped for column in pk_columns):
        candidates.append((True, pk_columns))

    for column in table.columns:
        if column.unique and column.name in mapped:
            candidates.append((False, (column.name,)))

    unique_candidates = list(dict.fromkeys(candidates))
    unique_candidates.sort(key=lambda candidate: (not candidate[0], len(candidate[1])))
    return [columns for _, columns in unique_candidates]


def matching_key_index_name(table: TableMetadata, key_columns: Sequence[str]) -> Optional[str]:
    for key in table.unique_keys:
        if tuple(key.columns) == tuple(key_columns):
            return key.name
    return None


def is_missing_key_value(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        return value.strip().lower() in ("", "nan", "none", "null", "nat")
    return False


def normalize_key_value(value: Any, column: ColumnMetadata) -> Optional[Tuple[str, str]]:
    if is_missing_key_value(value):
        return None

    raw_value = value.strip() if isinstance(value, str) else value
    column_type = column.type.upper()
    if any(token in column_type for token in ("INT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "MONEY")):
        try:
            number = Decimal(str(raw_value).replace(",", "").replace("$", ""))
            if number.is_finite():
                return ("number", format(number.normalize(), "f"))
        except (InvalidOperation, ValueError):
            return None
    if isinstance(raw_value, (int, float, Decimal)) and not isinstance(raw_value, bool):
        try:
            number = Decimal(str(raw_value))
            if number.is_finite():
                return ("number", format(number.normalize(), "f"))
        except InvalidOperation:
            return None
    if hasattr(raw_value, "isoformat"):
        return ("date", raw_value.isoformat())
    text_value = str(raw_value)
    if column.collation and "_CI_" in column.collation.upper():
        text_value = text_value.casefold()
    return ("text", text_value)


def canonical_key(values: Sequence[Any], columns: Sequence[ColumnMetadata]) -> Optional[CanonicalKey]:
    if len(values) != len(columns):
        return None
    normalized = [normalize_key_value(value, column) for value, column in zip(values, columns)]
    if any(value is None for value in normalized):
        return None
    return tuple(normalized)  # type: ignore[arg-type]


def source_key(
    row: Dict[str, Any],
    key_columns: Sequence[str],
    database_to_source: Dict[str, str],
    column_metadata: Dict[str, ColumnMetadata]
) -> Optional[CanonicalKey]:
    values = [row.get(database_to_source[column]) for column in key_columns]
    columns = [column_metadata[column] for column in key_columns]
    return canonical_key(values, columns)


def load_existing_records(
    connection: Connection,
    engine: Engine,
    table: TableMetadata,
    key_columns: Sequence[str],
    source_keys: Sequence[Sequence[Any]],
    primary_key_columns: Sequence[str],
    additional_columns: Sequence[str] = (),
    batch_size: int = 200,
    lock_key_ranges: bool = False,
    key_index_name: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Fetch only rows matching source keys in bounded batches; never scan a target table."""
    column_metadata = {column.name: column for column in table.columns}
    key_metadata = [column_metadata[column] for column in key_columns]
    pk_columns = list(primary_key_columns)
    distinct_values: Dict[CanonicalKey, Sequence[Any]] = {}
    for values in source_keys:
        key = canonical_key(values, key_metadata)
        if key is not None:
            distinct_values.setdefault(key, values)

    if not distinct_values:
        return []

    dialect = engine.name
    table_sql = quote_table(table, dialect)
    selected = list(dict.fromkeys([*key_columns, *pk_columns, *additional_columns]))
    selected_sql = ", ".join(_quote_identifier(name, dialect) for name in selected)
    key_items = list(distinct_values.items())
    max_keys = max(1, min(batch_size, 1800 // max(1, len(key_columns))))

    matched_rows: List[Dict[str, Any]] = []
    for offset in range(0, len(key_items), max_keys):
        chunk = key_items[offset:offset + max_keys]
        params: Dict[str, Any] = {}
        predicates: List[str] = []
        for key_index, (_, values) in enumerate(chunk):
            components = []
            for col_index, (column_name, value) in enumerate(zip(key_columns, values)):
                bind_name = f"k_{key_index}_{col_index}"
                params[bind_name] = value
                components.append(f"{_quote_identifier(column_name, dialect)} = :{bind_name}")
            predicates.append("(" + " AND ".join(components) + ")")

        table_ref = table_sql
        if lock_key_ranges and dialect == "mssql":
            hints = "UPDLOCK, HOLDLOCK"
            if key_index_name:
                hints += f", INDEX({_quote_identifier(key_index_name, dialect)})"
            table_ref += f" WITH ({hints})"
        query = text(f"SELECT {selected_sql} FROM {table_ref} WHERE " + " OR ".join(predicates))
        matched_rows.extend(dict(row) for row in connection.execute(query, params).mappings().all())

    if not pk_columns:
        return matched_rows
    unique_rows: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for row in matched_rows:
        primary_key = tuple(row.get(column) for column in pk_columns)
        unique_rows[primary_key] = row
    return list(unique_rows.values())


def load_existing_matches(
    connection: Connection,
    engine: Engine,
    table: TableMetadata,
    key_columns: Sequence[str],
    source_keys: Sequence[Sequence[Any]],
    primary_key_columns: Sequence[str],
    batch_size: int = 200,
    lock_key_ranges: bool = False,
    key_index_name: Optional[str] = None
) -> Dict[CanonicalKey, Set[Tuple[Any, ...]]]:
    """Index bounded source-key candidate rows by canonical key and actual primary key."""
    column_metadata = {column.name: column for column in table.columns}
    key_metadata = [column_metadata[column] for column in key_columns]
    result: Dict[CanonicalKey, Set[Tuple[Any, ...]]] = {}
    records = load_existing_records(
        connection=connection,
        engine=engine,
        table=table,
        key_columns=key_columns,
        source_keys=source_keys,
        primary_key_columns=primary_key_columns,
        batch_size=batch_size,
        lock_key_ranges=lock_key_ranges,
        key_index_name=key_index_name
    )
    for row in records:
        key = canonical_key([row.get(name) for name in key_columns], key_metadata)
        if key is not None:
            primary_key = tuple(row.get(name) for name in primary_key_columns)
            result.setdefault(key, set()).add(primary_key)

    return result