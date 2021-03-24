## Table of contents
* [General Info](#general-info)
* [Description](#description)
* [Technologies](#technologies)
* [Setup](#setup)
* [Screenshot](#screenshot)


## General Info
Step Four: Analytical ETL for Guided Capstone project.


<hr/>

## Description
This step focuses on using SparkSQL and Python to build an ETL job that calculates the following results for a given day:
- Latest trade price before the quote.
- Latest 30-minute moving average trade price, before the quote.
- The bid/ask price movement from the previous day’s closing price.

<hr/>

## Technologies
The Project is created with the following technologies:
* Azure Storage-Containers/File Share: To store the raw data
* Databricks-connect: Allow the user to step through and debug Spark code in the local environment and execute it on remote Azure Databricks cluster (7.3 LTS)
    * [Reference](https://docs.databricks.com/dev-tools/databricks-connect.html)
       * Python 3.7.5 (which matches the remote cluster python version)
    


## Setup

* Create a Databricks cluster and notebook and run the code in files 'databricks.py' to mount the raw data files from Azure Blob Container to DBFS

Navigate to the project folder and execute the following commands

* Pre-processing step to work on the raw files : 'dbfs:/mnt/FileStore/raw/'

```
python process_raw.py

```

* The driver will call the transformation code for executing the ETL load to load end of day daily records for trade and quote and finally perform analytical transformations on those records to get the final quote table

```
python etldriver.py

```

* Python files:

<hr/>

   * <b>process_raw.py</b>: Preprocessing step to process the trade and quote records from stock exchange daily submissions files in a semi-structured text format.

   * <b>etl_load.py</b>: After preprocessing the incoming data from the exchange, we need to further process and load the final quote and trade records.

   * <b>analytical_transformations.py</b>: Logic to build analytical quote table based on current and previous day trade calculations.

<hr/>



### Screenshot

* Trade moving average records

![Alt text](Screenshot/Trade_moving_avg.PNG?raw=true "Trade_moving_avg")

*  Prior Day Trade price per exchange/symbol

![Alt text](Screenshot/last_trade_pr_exchange_symbol.PNG?raw=true "last_trade_pr_exchange_symbol")

* Quote update record table with latest trade and moving trade price

![Alt text](Screenshot/quote_update.PNG?raw=true "quote_update")

* Final Quote Table with Prior Day close price

![Alt text](Screenshot/final_quote.PNG?raw=true "final_quote")
