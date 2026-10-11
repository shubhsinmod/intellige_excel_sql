import json
from app.database.connection import get_engine
from app.database.metadata import extract_database_metadata
from app.mapping.matcher import ColumnMatcher

engine = get_engine()
schema = extract_database_metadata(engine)
with open('uploads/upload_9fb1ee74487f_session.json') as f:
    session = json.load(f)
cols = session['columns']
results = ColumnMatcher.match_columns(cols, schema)
for r in results:
    cand_str = ''
    if r.candidates:
        cand_str = ' -> candidates: ' + ', '.join([f"{c['databaseTable']}.{c['databaseColumn']} ({c['confidence']})" for c in r.candidates])
    print(f'{r.excel_column:20} -> {r.database_table:15}.{r.database_column:20} [{r.status}] ({r.confidence}) {cand_str}')
