import json

with open('/Users/bhaskarkandala/python_venv/config/file_to_bq_tbl_map.json', 'r') as f:
    tables = json.load(f)
    print(tables)
