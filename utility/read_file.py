import re
with open('/Users/bhaskarkandala/python_venv/config/sql/batting_stats.sql','r') as reader:
    data=reader.read()
    sql=[q.strip() for q in data.split(';') if q.strip()]
    for idx, query in enumerate(sql):
       pattern = r'`(?:[^`]*\.)?([^.`]+)`'
       match = re.search(pattern,query)
       #print(match)
       table_name = match.group(1)
       print(table_name)
    #CREATE TABLE IF NOT EXISTS `vocal-chiller-457916-r2.ipl_data_dwh.matches_info` 


   
         #   match = re.search(r'`[^`]*\.(\w+)`\s*\(', query)
         #   table_name = match.group(1).strip().replace('.', '_')  if match else f"table_{idx}"