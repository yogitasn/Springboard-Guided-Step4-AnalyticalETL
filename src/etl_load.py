import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType,DecimalType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains,date_format,regexp_replace
from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql import Window
from pyspark.sql import functions as F
import logging
import configparser

logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d \
:: %(message)s', level = logging.INFO)

config = configparser.ConfigParser()
config.read('..\config.cfg')

class ETLLoad:
    """
    This class performs transformation operations on the dataset.
    Transform timestamp format, clean text part, remove extra spaces etc
    """        
    def __init__(self,spark):
        self.spark=spark
        self._load_path = config.get('BUCKET', 'WORKING_ZONE')
        self._save_path = config.get('BUCKET', 'PROCESSED_ZONE')
        

    # End of load of trade records
    def etl_load_trade(self):
        logging.debug("Inside transform parking occupancy dataset module")

        trade_common = self.spark.read.\
                       parquet(self._load_path+"/partition=T/*.parquet")

        trade = trade_common.select("trade_dt", "symbol", "exchange",\
                             "event_tm","event_seq_nb", "arrival_tm", "trade_pr")
        
        


        trade_corrected=trade.withColumn("row_number",F.row_number().over(Window.partitionBy(trade.trade_dt,\
                        trade.symbol,trade.exchange,trade.event_tm,trade.event_seq_nb) \
                        .orderBy(trade.arrival_tm.desc()))).filter(F.col("row_number")==1).drop("row_number")
                
        

        trade_corrected.show(3,truncate=False)
        
        logging.debug("Writting transformed trade dataframe to a trade partition")
        trade_corrected.withColumn("trade_date", F.col("trade_dt")).write.\
                       partitionBy("trade_dt").mode("overwrite").parquet(self._save_path+"/trade/")


     # End of load of quote records
    def etl_load_quote(self):
        logging.debug("Inside transform parking occupancy dataset module")

        quote_common = self.spark.read.\
                       parquet(self._load_path+"/partition=Q/*.parquet")
 
        quote=quote_common.select("trade_dt","symbol","exchange","event_tm","event_seq_nb","arrival_tm", \
                                 "bid_pr","bid_size","ask_pr","ask_size")

        quote_corrected=quote.withColumn("row_number",F.row_number().over(Window.partitionBy(quote.trade_dt,quote.symbol,\
                                            quote.exchange,quote.event_tm,quote.event_seq_nb).\
                                            orderBy(quote.arrival_tm.desc()))).filter(F.col("row_number")==1).drop("row_number")


        quote_corrected.show(3,truncate=False)

        quote_corrected.printSchema()

        logging.debug("Writting transformed quote dataframe to a trade partition")
        quote_corrected.withColumn("trade_date", F.col("trade_dt")).write.\
                       partitionBy("trade_dt").mode("overwrite").parquet(self._save_path+"/quote/")

