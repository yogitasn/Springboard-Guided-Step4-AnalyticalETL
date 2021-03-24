import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext 
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType,DecimalType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains,date_format,regexp_replace
import logging
import configparser
from pathlib import Path
from datetime import datetime, timedelta
from datetime import date
from pyspark.sql import Window
from pyspark.sql import functions as F

import datetime
from typing import List
from decimal import Decimal

from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType, DecimalType

import json
from decimal import Decimal 
import datetime


logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d \
:: %(message)s', level = logging.INFO)

config = configparser.ConfigParser()
config.read('..\config.cfg')

class Preprocess:
    """
    This class performs transformation operations on the dataset.
    Transform timestamp format, clean text part, remove extra spaces etc
    """        
    def __init__(self):
        
        self._load_path = config.get('BUCKET', 'RAW_ZONE')
        self._save_path = config.get('BUCKET', 'PREPROCESSED_ZONE')
        

    # Tranforms the datasets from 2018 to 2020 as the data has same pattern
    @staticmethod
    def parse_csv(line):
        """
        Function to parse comma seperated records
        
        """        
        try:
            # position of the record_type field
            record_type_pos = 2

            # get the common fields applicable to both 'Q' and 'T' type records
            record = line.split(",")
            trade_dt= datetime.datetime.strptime(record[0], '%Y-%m-%d')
            arrival_tm=datetime.datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f')
            rec_type=record[2]
            symbol=record[3]
            exchange=record[6]
            event_tm=datetime.datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f')
            event_seq_nb=int(record[5])
            
            # fields specific to record_type is 'T'
            if record[record_type_pos] == "T":
                trade_pr=Decimal(record[7])
                event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,arrival_tm,\
                        trade_pr,Decimal('0.0'),0,Decimal('0.0'),0,"T")
                return event
            
            # fields specific to record_type is 'Q'
            elif record[record_type_pos] == "Q":
                bid_pr=Decimal(record[7])
                bid_size=int(record[8])
                ask_pr=Decimal(record[9])
                ask_size=int(record[10])

                event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,arrival_tm,\
                        Decimal('0.0'),bid_pr,bid_size,ask_pr,ask_size,"Q")
                return event
        # capture record in a bad partition if any error occurs
        except Exception as e:
                return ("","","","","","","","","","","","B",line)

    def process_csv(self):

        # schema to parse both Q and T type records
        common_event = StructType() \
                    .add("trade_dt",DateType(),True) \
                    .add("rec_type",StringType(),True) \
                    .add("symbol",StringType(),True) \
                    .add("exchange",StringType(),True) \
                    .add("event_tm",TimestampType(),True) \
                    .add("event_seq_nb",IntegerType(),True) \
                    .add("arrival_tm",TimestampType(),True) \
                    .add("trade_pr",DecimalType(17,14),True) \
                    .add("bid_pr",DecimalType(17,14),True) \
                    .add("bid_size",IntegerType(),True) \
                    .add("ask_pr",DecimalType(17,14),True) \
                    .add("ask_size",IntegerType(),True) \
                    .add("partition",StringType(),True)
        
        spark = SparkSession.builder.master('local').\
                appName('app').getOrCreate()
                    

        raw = spark.sparkContext.\
                textFile(self._load_path+"/csv/*/NYSE/*.txt")

        # Parse the text file and parse using the parse_csv function to get the rdd in proper format. 
        parsed = raw.map(lambda line: self.parse_csv(line))
        data_csv = spark.createDataFrame(parsed,schema=common_event)

        data_csv.show(truncate=False)

        # Save the final dataframe as parquet files in partitions
        data_csv.write.partitionBy("partition").mode("append").parquet(self._save_path)

    @staticmethod
    def parse_json(line:str):
        """
        Function to parse json records
        
        """
        try:
            # built-in function to parse the json file
            record = json.loads(line)
            
            # Common fields applicable to both 'Q' and 'T' type records
            record_type = record['event_type']
            trade_dt= datetime.datetime.strptime(record['trade_dt'], '%Y-%m-%d')
            arrival_tm=datetime.datetime.strptime(record['file_tm'], '%Y-%m-%d %H:%M:%S.%f')
            rec_type=record_type
            symbol=record['symbol']
            exchange=record['exchange']
            event_tm=datetime.datetime.strptime(record['event_tm'], '%Y-%m-%d %H:%M:%S.%f')
            event_seq_nb=int(record['event_seq_nb'])

            # capturing field values specific to "T" type records
            if record_type == "T":
                trade_pr=Decimal(record['price'])
                event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,arrival_tm,\
                        trade_pr,Decimal('0.0'),0,Decimal('0.0'),0,"T")
                return event
            
            # capturing field values specific to "Q" type records
            elif record_type == "Q":
                bid_pr=Decimal(record['bid_pr'])
                bid_size=int(record['bid_size'])
                ask_pr=Decimal(record['ask_pr'])
                ask_size=int(record['ask_size'])
                event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,arrival_tm,\
                        Decimal('0.0'),bid_pr,bid_size,ask_pr,ask_size,"Q")
                return event

        # capture record in a bad partition if any error occurs
        except Exception as e:
                return ("","","","","","","","","","","","B",line)


    def process_json(self):
    
        # schema to parse both Q and T type records
        common_event = StructType() \
                    .add("trade_dt",DateType(),True) \
                    .add("rec_type",StringType(),True) \
                    .add("symbol",StringType(),True) \
                    .add("exchange",StringType(),True) \
                    .add("event_tm",TimestampType(),True) \
                    .add("event_seq_nb",IntegerType(),True) \
                    .add("arrival_tm",TimestampType(),True) \
                    .add("trade_pr",DecimalType(17,14),True) \
                    .add("bid_pr",DecimalType(17,14),True) \
                    .add("bid_size",IntegerType(),True) \
                    .add("ask_pr",DecimalType(17,14),True) \
                    .add("ask_size",IntegerType(),True) \
                    .add("partition",StringType(),True)
        spark = SparkSession.builder.master('local').\
                appName('app').getOrCreate()

        raw = spark.sparkContext.textFile(self._load_path+"/json/*/NASDAQ/*.txt")
        parsed = raw.map(lambda line: self.parse_json(line))
        data_json = spark.createDataFrame(parsed,schema=common_event)
        data_json.show(truncate=False)

        # Save the final dataframe as parquet files in partitions
        data_json.write.partitionBy("partition").mode("append").parquet(self._save_path)


pre_process= Preprocess()
pre_process.process_csv()

pre_process.process_json()
