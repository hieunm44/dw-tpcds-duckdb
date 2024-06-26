import duckdb
import time

sc = 1
delete_file = f'../../refreshed_data/scale_{sc}/delete_1.dat'
con = duckdb.connect(f'../../created_db/scale_{sc}.db')

with open(delete_file, 'r') as dat_file:
    for row in dat_file:
        row = [value.strip() for value in row.split('|')]
        start_date = row[0]
        end_date = row[1]
        start_time = time.time()
        
        print(f'The Deletion for DF_WS started: {start_time}, for date {start_date} and {end_date}')   
             
        con.sql(f"DELETE FROM web_sales USING date_dim WHERE web_sales.ws_sold_date_sk = date_dim.d_date_sk AND date_dim.d_date BETWEEN CAST('{start_date}' AS DATE) AND CAST('{end_date}' AS DATE);")                
        con.sql(f" DELETE FROM web_returns USING date_dim WHERE web_returns.wr_returned_date_sk = date_dim.d_date_sk AND date_dim.d_date BETWEEN CAST('{start_date}' AS DATE) AND CAST('{end_date}' AS DATE);")                
                
        end_time = time.time()
        print(end_time-start_time)
        print(f'The time for the delete (DF_WS) took: {end_time-start_time} seconds')



con.close()