import duckdb
import time


def create_wrv(con):
    con.sql('''CREATE VIEW IF NOT EXISTS wrv AS
SELECT d_date_sk wr_return_date_sk
 ,t_time_sk wr_return_time_sk
 ,i_item_sk wr_item_sk
 ,c1.c_customer_sk wr_refunded_customer_sk
 ,c1.c_current_cdemo_sk wr_refunded_cdemo_sk
 ,c1.c_current_hdemo_sk wr_refunded_hdemo_sk
 ,c1.c_current_addr_sk wr_refunded_addr_sk
 ,c2.c_customer_sk wr_returning_customer_sk
 ,c2.c_current_cdemo_sk wr_returning_cdemo_sk
 ,c2.c_current_hdemo_sk wr_returning_hdemo_sk
 ,c2.c_current_addr_sk wr_returing_addr_sk
 ,wp_web_page_sk wr_web_page_sk 
 ,r_reason_sk wr_reason_sk
 ,wret_order_id wr_order_number
 ,wret_return_qty wr_return_quantity
 ,wret_return_amt wr_return_amt
 ,wret_return_tax wr_return_tax
 ,wret_return_amt + wret_return_tax AS wr_return_amt_inc_tax
 ,wret_return_fee wr_fee
 ,wret_return_ship_cost wr_return_ship_cost
 ,wret_refunded_cash wr_refunded_cash
 ,wret_reversed_charge wr_reversed_charge
 ,wret_account_credit wr_account_credit
 ,wret_return_amt+wret_return_tax+wret_return_fee
 -wret_refunded_cash-wret_reversed_charge-wret_account_credit wr_net_loss
FROM s_web_returns LEFT OUTER JOIN date_dim ON (cast(wret_return_date as date) = d_date)
LEFT OUTER JOIN time_dim ON ((CAST(SUBSTR(wret_return_time,1,2) AS integer)*3600
+CAST(SUBSTR(wret_return_time,4,2) AS integer)*60+CAST(SUBSTR(wret_return_time,7,2) AS integer))=t_time)
LEFT OUTER JOIN item ON (wret_item_id = i_item_id)
LEFT OUTER JOIN customer c1 ON (wret_return_customer_id = c1.c_customer_id)
LEFT OUTER JOIN customer c2 ON (wret_refund_customer_id = c2.c_customer_id)
LEFT OUTER JOIN reason ON (wret_reason_id = r_reason_id)
LEFT OUTER JOIN web_page ON (wret_web_page_id = WP_WEB_PAGE_id)
WHERE i_rec_end_date IS NULL AND wp_rec_end_date IS NULL;''')

def insert_wrv(con):
    con.sql("insert or ignore into web_returns select distinct(*) from wrv")


if __name__ == "__main__":
    sc = 1
    con = duckdb.connect(f'../../created_db/scale_{sc}.db')
    
    start_time = time.time()
    print(f'The Insertion for LF_WR started: {start_time}')
    create_wrv(con)
    insert_wrv(con)
    end_time = time.time()
    print(f'The time for the Insertion (LF_WR) took: {end_time-start_time} seconds')


