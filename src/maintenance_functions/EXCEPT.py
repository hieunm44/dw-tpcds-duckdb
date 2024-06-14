import duckdb
import time


# this function is to simulate failure
sc = 1
delete_file = f'../../refreshed_data/scale_{sc}/delete_1.dat'
con = duckdb.connect(f'../../created_db/scale_{sc}.db')
 
           
con.sql(f"DELETE FROM web_sales USING date_dim_fd WHERE web_sales.ws_sold_date_sk = date_dim.d_date_sk ")             
con.sql(f" DELETE FROM web_returns USING date_dim_df WHERE web_returns.wr_returned_date_sk = date_dim.d_date_sk ")



con.close()