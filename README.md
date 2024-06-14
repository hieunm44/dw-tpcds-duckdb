# Data Warehouses Project 1 - TPC-DS Benchmark Using DuckDB

<div align="center">
<a href="https://www.bruface.eu/">
    <img src="https://www.bruface.eu/sites/default/files/BUA-BRUFACE_RGB_Update_DEF_1_0_0.jpg" height=100"/>
</a>
</div>

## Overview
This repo is our project "TPC-DS Benchmark Using DuckDB" in the course "Data Warehouses" at Université Libre de Bruxelles (ULB). In this project, we implement the [TPC-DS Benchmark](https://www.tpc.org/tpcds/) on [DuckDB](https://duckdb.org/) Database Management System.

## Setup
1. Clone the repo
   ```sh
   git clone https://github.com/hieunm44/dw-tpcds-duckdb.git
   cd dw-tpcds-duckdb
   ```
2. Install `duckdb` package
   ```sh
   pip install duckdb
   ```
3. Go to https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp and download `TPC-DS_Tools_v3.2.0.zip`, then unzip it to a folder `tpcds-kit`.
4. Check the document `TPC-DS_v3.2.0.pdf` (also from the link above) to get details about the TPC-DS benchmark.
5. Give full access permission to data folders
   ```sh
   chmod 777 generated_data
   chmod 777 generated_queries
   chmod 777 refreshed_data
   ```

## Usage
We only show examples for scale factor 1. Other scales can be reimplemented similarly.
1. Data generation
   ```sh
   cd tpcds-kit/tools
   make LINUX_CC=gcc-9 OS=LINUX
   ./dsdgen -scale 1 -dir "../../generated_data/scale_1" -suffix .csv -verbose Y -force Y
   # Notes: folder path must not contain spaces
   ```
   Then 25 `.csv` files will be generated in the folder `refreshed_data/scale_1`.
2. Query generation
   ```sh
   ./dsqgen -directory "../query_templates/" -input "../query_templates/templates.lst" -dialect netezza -scale 1 -output_dir "../../generated_queries" -verbose Y
   ```
   Then a file `query_0.sql` containing 99 queries will be created in the folder `generated_queries`. Now we split all queries into 99 separate files, named from `query_1.sql` to `query_99.sql`.
   ```sh
   cd ../../generated_queries
   python3 split_queries.py
   ```  
3. Create database \
   Go to folder `src/create_database`, then run:
   ```sh
   python3 create_db.py
   ```
   A DuckDB database file `scale_1.db` will be created in the folder `created_db`.
4. Load data and load test \
   Go to folder `src/load_data`, then run:
   ```sh
   python3 load_data.py
   ```
   This script will load data from generated `.csv` files to tables in our databases, then it will give us load time.
5. Power test \
   Go to folder `src/test`, then run:
   ```sh
   python3 power_test.py
   ```
   This script measures running time of individual query and also the total time of running 99 queries.
6. Throughput test
   ```sh
   python3 throughput_test.py
   ```
   The script will return the throughput test time.
7. Maintenance test \
   Generate the dataset again as refreshed data:
   ```sh
   ./dsdgen -scale 1 -dir "../../refreshed_data/scale_1" -suffix .dat -update 1 -verbose Y -force Y
   ```
   Then 23 `.dat` files will be generated in the folder `refreshed_data/scale_1`. \
   Next, run two files `src/create_database/create_mtnc.py` and `src/load_data/load_mtnc.py` to create the database and load the dataset again for mantenence test. \
   Finally, go to folder `src/test` and run the test:
   ```sh
   python3 maintenance_test.py
   ```
   The script will give time for running maintenance functions.
