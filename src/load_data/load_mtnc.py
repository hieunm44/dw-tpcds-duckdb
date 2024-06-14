import duckdb
import os
import time

# specify the scale factor
sc = 1
con = duckdb.connect(f'../../created_db/scale_{sc}.db')

# specify the path for dataset
dat_folder_name = f'../../refreshed_data/scale_{sc}'
dat_list = os.listdir(dat_folder_name)

for dat_file in dat_list:
    if dat_file.startswith("s_"):
        table_name = dat_file[:-6]
        copy_command = f"COPY {table_name} FROM '{dat_folder_name}/{dat_file}' (DELIMITER '|')"
        con.sql(copy_command)

