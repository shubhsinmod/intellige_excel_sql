import time
from app.services.migration_service import migration_service

upload_id = "upload_9fb1ee74487f"
t0 = time.time()
print("Starting preview run...")
res = migration_service.process_step3(upload_id, force_refresh=True)
t1 = time.time()
print(f"Preview completed in {t1 - t0:.3f}s")
print(f"Matched columns count: {res.matched_columns_count}")
print(f"Tables processed: {res.tables_processed}")
print(f"Execution order: {res.execution_order}")
print(f"Value Resolution Summary: {res.value_resolution_summary}")
print(f"Validation summary: canExecute={res.can_execute}, totalRows={res.validation_summary.total_rows}, validRows={res.validation_summary.valid_rows}, invalidRows={res.validation_summary.invalid_rows}, errors={len(res.validation_summary.errors)}")
if res.validation_summary.errors:
    print("Top errors:")
    for e in res.validation_summary.errors[:5]:
        print("  -", e.table_name, e.column_name, e.error_type, e.message)
