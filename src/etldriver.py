import pyspark
from pyspark.sql import SparkSession
from pathlib import Path
import logging
import logging.config
import configparser
import time
from etl_load import ETLLoad
from analytical_transformations import AnalyticalETL
#from process_raw import Preprocess

def create_sparksession():
    """
    Initialize a spark session
    """
    return SparkSession.builder.\
            appName("Spring Capital Analytical ETL").\
            getOrCreate()

            

def main():
    """
    Driver code to perform the following steps
    1. Works on the pre-processed parquet files and transforms and loads the end of day trade and quote records
    2. Executes the final analytical_etl function that uses SparkSQL and Python to build an ETL job 
        that calculates the following results for a given day:
        - Latest trade price before the quote.
        - Latest 30-minute moving average trade price, before the quote.
        - The bid/ask price movement from previous day’s closing price
    """
    logging.debug("\n\nSetting up Spark Session...")
    spark = create_sparksession()
    etl_load = ETLLoad(spark)
    analy_etl = AnalyticalETL(spark)

    # cleaning tables
    logging.debug("\n\nCleaning Hive Tables...")
    spark.sql("DROP TABLE IF EXISTS trade")
    spark.sql("DROP TABLE IF EXISTS temp_trade")
    spark.sql("DROP TABLE IF EXISTS temp_trade_mov_avg")
    spark.sql("DROP TABLE IF EXISTS prev_trade")
    spark.sql("DROP TABLE IF EXISTS prev_temp_last_trade")
    spark.sql("DROP TABLE IF EXISTS temp_quote")
    spark.sql("DROP TABLE IF EXISTS quotes")
    spark.sql("DROP TABLE IF EXISTS quotes_union")
    spark.sql("DROP TABLE IF EXISTS trades_latest")
    spark.sql("DROP TABLE IF EXISTS quotes_update")
    

    # Modules in the project
    modules = {
         "trade.parquet": etl_load.etl_load_trade,
         "quote.parquet" : etl_load.etl_load_quote,
         "analytical_quote": analy_etl.anaytical_etl
    }

    for file in modules.keys():
        modules[file]()
    
   
        

# Entry point for the pipeline
if __name__ == "__main__":
    main()