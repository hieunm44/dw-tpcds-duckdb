import duckdb
import os
import time

# specify the scale factor
sc = 1
con = duckdb.connect(f'../../created_db/scale_{sc}.db')

# specify the path for dataset
csv_folder_name = f'../../generated_data/scale_{sc}'
csv_list = os.listdir(csv_folder_name)

# set the timer start
start_time = time.time()
for csv_file in csv_list:
    table_name = csv_file[:-4]
    # load data into tables
    copy_command = f"COPY {table_name} FROM '{csv_folder_name}/{csv_file}' (DELIMITER '|')"
    con.sql(copy_command)
# set the timer end
end_time = time.time()
time_taken = end_time - start_time
print(time_taken)