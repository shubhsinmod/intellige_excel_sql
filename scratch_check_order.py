import sys
sys.path.insert(0, 'backend')
from app.services.database_service import database_service

schema = database_service.get_schema_metadata()
order_tbl = next((t for t in schema.tables if t.name.lower() in ('order', 'orders')), None)
print('ORDER TABLE NAME:', order_tbl.name if order_tbl else 'NONE')
if order_tbl:
    for c in order_tbl.columns:
        print(f"  {c.name}: type={c.type}, nullable={c.nullable}, identity={c.identity}, pk={c.primary_key}, default={c.default}")

print("\n--- Outlets TABLE ---")
outlets_tbl = next((t for t in schema.tables if t.name.lower() in ('outlets', 'outlet')), None)
if outlets_tbl:
    for c in outlets_tbl.columns:
        print(f"  {c.name}: type={c.type}, nullable={c.nullable}, identity={c.identity}, pk={c.primary_key}, default={c.default}")
