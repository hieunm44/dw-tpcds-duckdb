
import duckdb
import os
import fnmatch
import time
import pandas as pd
import pickle


# specify the scale factor
sc = 1
start_time = time.time()
print(f'Start Excution {start_time} seconds')
con = duckdb.connect(f'../../created_db/scale_{sc}.db')

list_time = {}
# read and run all the queries
for root, dirnames, filenames in os.walk('../../generated_queries/'):
    for filename in fnmatch.filter(filenames, '*.sql'):
        if filename != 'query_0.sql':
            t= time.time()
            sql = open(f'{root}/{filename}','r')
            sql = sql.read()
            try:
                # run all the queries
                result = con.execute(sql)
                close = time.time()
                print(f"successfully executed! {filename} with time {close - t} seconds")
                list_time[filename] = close - t
            except Exception as e:
                print(f"problem executing {filename}!: {e}") 
                
# Calculate the elapsed time
end_time= time.time()     
elapsed_time = end_time -start_time
print(f'Total time is {elapsed_time} seconds')  
print(list_time)
          