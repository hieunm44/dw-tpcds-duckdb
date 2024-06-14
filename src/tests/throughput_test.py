import duckdb
import os
import fnmatch
import time
import threading

# Shared database connection
sc = 1
db_connection = duckdb.connect(f'../../created_db/scale_{sc}.db')

def execute_query(query, file_name):
    global db_connection  # Use the shared database connection
    start_time = time.time()
    print(f"Executing query from thread ID: {threading.get_ident()} for file: {file_name}")
    
    if db_connection is None:
        db_connection = duckdb.connect(f'../../created_db/scale_{sc}.db')
    
    cursor = db_connection.cursor()
    cursor.execute(query)
    
    # Calculate the elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'Total time is {elapsed_time} seconds') 
    print(f"Done executing query from thread ID: {threading.get_ident()} for file: {file_name}")
    
    
def main():
    outputfile = "output.txt"
    
    TP_test_start_time_1 = time.time()

    queries = []
    threads = []
    
    for root, dirnames, filenames in os.walk('../../generated_queries/'):
        for filename in fnmatch.filter(filenames, '*.sql'):
            if filename != 'query_0.sql':
                sql_file = open(os.path.join(root, filename), 'r')
                query = sql_file.read()
                queries.append((query, filename))  # Store query along with filename 
    
    
    # create threads and assign query stream to threads
    for query, file_name in queries:
        thread = threading.Thread(target=execute_query, args=(query, file_name))
        threads.append(thread)
        thread.start()    

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # calculate the time for throughput test
    TP_test_end_time_1 = time.time()
    TP_test_time_1 = TP_test_end_time_1 - TP_test_start_time_1
    output = f'THROUGHPUT TEST TIME:\n\tThroughput test start time = {TP_test_start_time_1}\n\tThroughput test end time = {TP_test_end_time_1}\n\tThroughput test time = {TP_test_time_1}\n'
    print(output)

    if outputfile:
        with open(outputfile, 'w+') as f:
            f.write(output)

if __name__ == "__main__":
    main()

