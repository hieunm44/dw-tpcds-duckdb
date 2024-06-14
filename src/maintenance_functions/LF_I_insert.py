import duckdb
import time


def create_iv(con):
    con.sql('''CREATE view IF NOT EXISTS iv AS
SELECT d_date_sk inv_date_sk,
 i_item_sk inv_item_sk,
 w_warehouse_sk inv_warehouse_sk,
 invn_qty_on_hand inv_quantity_on_hand
FROM s_inventory
LEFT OUTER JOIN warehouse ON (invn_warehouse_id=w_warehouse_id)
LEFT OUTER JOIN item ON (invn_item_id=i_item_id AND i_rec_end_date IS NULL)
LEFT OUTER JOIN date_dim ON (d_date=invn_date);''')

def insert_iv(con):
    con.sql("insert or ignore into inventory select distinct(*) from iv")


if __name__ == "__main__":
    sc = 1
    con = duckdb.connect(f'../../created_db/scale_{sc}.db')
    
    start_time = time.time()
    print(f'The Insertion for LF_I started: {start_time}')
    create_iv(con)
    insert_iv(con)
    end_time = time.time()
    print(f'The time for the Insertion (LF_I) took: {end_time-start_time} seconds')


