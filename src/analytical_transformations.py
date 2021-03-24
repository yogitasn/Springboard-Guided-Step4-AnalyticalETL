import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType,DecimalType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.functions import col,array_contains,date_format,regexp_replace
from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql.functions import udf
import logging
import configparser


logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d \
:: %(message)s', level = logging.INFO)

config = configparser.ConfigParser()
config.read('..\config.cfg')

class AnalyticalETL:
    """
        This class works on trade and quote tables populated with daily records extracted the previous etl_load step, 
        and focuses on using SparkSQL and Python to build an ETL job that calculates the following results for a given day:
        - Latest trade price before the quote.
        - Latest 30-minute moving average trade price, before the quote.
        - The bid/ask price movement from previous day’s closing price
        

    """        
    def __init__(self,spark):
        self.spark=spark
        self._load_path = config.get('BUCKET', 'WORKING_PROCESSED_ZONE')
        self._save_path = config.get('BUCKET', 'ANALYTICAL_PROCESSED_ZONE')
        

    def anaytical_etl(self):
        logging.debug("Analytical ETL")
        
        # reading the processed trade parquet files
        df = self.spark.read.parquet(self._load_path+"/trade/*/*.parquet")

        # creating trade staging table
        df.createOrReplaceTempView("temp_trade")

        # filter the trade records on the current date
        # Note: We are filtering records based on the data availability hence the trade_date is hardcoded below.

        df = self.spark.sql("SELECT trade_date, symbol, exchange, \
                                    event_tm, event_seq_nb, trade_pr \
                               FROM temp_trade \
                             WHERE trade_date='2020-08-06'")

        df.createOrReplaceTempView("trade")

        # Get the 30 minute moving average of trade price using temp view
        mov_avg_df = self.spark.sql("""SELECT t.*,avg(t.trade_pr) OVER(PARTITION BY symbol,exchange ORDER BY event_tm DESC
                                              RANGE BETWEEN INTERVAL '30' MINUTES PRECEDING AND CURRENT ROW) AS moving_average
                                          FROM trade as t;""")
        
        mov_avg_df.write.saveAsTable("temp_trade_mov_avg")

   
        # Creating temporary view for last day trading
        date = datetime.strptime('2020-08-05', '%Y-%m-%d')
        prev_date_str = date-timedelta(days=1)
        prev_date = prev_date_str.strftime('%Y-%m-%d')
        prev_date="2020-08-05"

        # Read the previous day's trade records
        prev_df= self.spark.sql("SELECT trade_date, symbol, exchange, \
                                        event_tm, event_seq_nb, trade_pr \
                                   FROM temp_trade where trade_date='2020-08-05'")

        prev_df.createOrReplaceTempView("prev_trade")

        # Get the previous day 30 minute moving trade price average per symbol, exchange and get the last trade price 
        prev_temp_last_trade = self.spark.sql("""SELECT * 
                                                   FROM 
                                                    (SELECT symbol, 
                                                                exchange,
                                                                last_trade_pr, \
                                                                RANK() OVER(partition by symbol, exchange ORDER BY event_tm DESC) rank \
                                                        FROM (SELECT symbol,
                                                                    exchange,
                                                                    event_tm,
                                                                    avg(trade_pr) OVER(PARTITION BY symbol, exchange ORDER BY event_tm DESC \
                                                                    RANGE BETWEEN INTERVAL '30' MINUTES PRECEDING AND CURRENT ROW) AS last_trade_pr \
                                                                FROM prev_trade) t ) t1
                                                 WHERE rank=1""")

        prev_temp_last_trade.createOrReplaceTempView("prev_temp_last_trade")



        # Read the quote parquet file records into a dataframe
        quote = self.spark.read.parquet(self._load_path+"/quote/*/*.parquet")

        quote.createOrReplaceTempView("temp_quote")


        # filter the quote records on the current date
        # Note: We are filtering records based on the data availability per dates hence the trade_date is hardcoded below.
        quotes = self.spark.sql("SELECT trade_date,symbol,exchange,\
                                        event_tm,event_seq_nb,bid_pr,\
                                        bid_size,ask_pr,ask_size \
                                   FROM temp_quote where trade_date='2020-08-06'")

        quotes.createOrReplaceTempView("quotes")

        # create a denormalized union both quotes and temp_trade_moving_avg
        # we need to join “quotes” and “temp_trade_moving_avg” to populate trade_pr and mov_avg_pr into quotes
        # we cannot use equality join as trade events don't happen at the same quote time
        quotes_union = self.spark.sql("""
                              SELECT trade_date,"Q" as rec_type,symbol,event_tm, \
                                     event_seq_nb,exchange,bid_pr,bid_size,ask_pr, \
                                     ask_size,NULL as trade_pr,NULL as moving_average \
                                FROM quotes
                              UNION
                              SELECT trade_date,"T" as rec_type,symbol,event_tm, \
                                     event_seq_nb,exchange,NULL as bid_pr,NULL as bid_size, \
                                     NULL as ask_pr,NULL as ask_size,trade_pr,moving_average \
                                FROM temp_trade_mov_avg
                            """)
  
        quotes_union.createOrReplaceTempView("quotes_union")
        
        
        # Get the last trade and moving average price for the current date
        trades_latest=self.spark.sql("""SELECT * 
                                            FROM (SELECT symbol,
                                                         exchange,
                                                         trade_pr as last_trade_pr,
                                                         moving_average as last_mov_avg_pr,
                                                         RANK () OVER ( 
                                                             PARTITION BY symbol, exchange
                                                             ORDER BY event_tm DESC
                                                         ) rank 
                                                    FROM  quotes_union
                                                  WHERE rec_type='T') t 
                                        WHERE rank =1;
                                    """)
        
        trades_latest.createOrReplaceTempView("trades_latest")

        # Filter the quote records after the above trade records calculation
        quotes_update = self.spark.sql("""SELECT q.trade_date, q.symbol, q.event_tm, 
                                                 q.event_seq_nb, q.exchange,q.bid_pr, q.bid_size,
                                                 q.ask_pr, q.ask_size, t.last_trade_pr, t.last_mov_avg_pr
                                            FROM quotes_union as q
                                            LEFT JOIN trades_latest as t
                                               ON q.exchange=t.exchange
                                               AND q.symbol=t.symbol
                                          WHERE q.rec_type = 'Q'
                                      """)


        quotes_update.createOrReplaceTempView("quotes_update")


        #  Join With Table prev_temp_last_trade To get The Prior Day Close Price
        quotes_final = self.spark.sql("""SELECT /*+ BROADCAST(prev_temp_last_trade) */ q.symbol, q.event_tm, 
                                                q.event_seq_nb, q.exchange, q.bid_pr, q.bid_size, 
                                                q.ask_pr, q.ask_size, q.bid_pr - prev_temp.last_trade_pr as bid_pr_mv, 
                                                q.ask_pr - prev_temp.last_trade_pr as ask_pr_mv, q.last_trade_pr, q.last_mov_avg_pr 
                                            FROM quotes_update as q
                                            LEFT OUTER JOIN prev_temp_last_trade as prev_temp
                                               ON q.exchange= prev_temp.exchange
                                               AND q.symbol = prev_temp.symbol
                                    """)

        
        # Write The Final Dataframe Into Azure Blob Storage At Corresponding Partition
        quotes_final.write.parquet(self._save_path+"/date={}".format("2020-08-06"))

        
      

