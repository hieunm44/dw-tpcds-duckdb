import re

input_file_path = 'query_0.sql'

with open(input_file_path, 'r') as f:
    sql_content = f.read()

pattern = re.compile(r'(-- start query \d+.*?)(?=-- start query \d+|$)', re.DOTALL)
queries = pattern.findall(sql_content)

for i, query in enumerate(queries, start=1):
    output_file_path = f'query_{i}.sql'
    with open(output_file_path, 'w') as f:
        f.write(query)

print("All queries have been successfully split into separate files.")
