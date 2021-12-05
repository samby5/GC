from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, FloatType
from datetime import datetime
import json
#spark = SparkSession.builder.master('local').\
#        appName('app').getOrCreate()
def parse_csv(line: str):
    record_type_pos = 2
    record = line.split(',')
    try:
        #common fields applicable to both 'Q' and 'T' type records
        trade_dt = datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f').date()
        rec_type = record[2]
        symbol = record[3]
        event_time = datetime.timestamp(datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'))
        event_seq_num = int(record[5])
        exchange = record[6]
        
        #fields specific to record_type 'T'
        if record[record_type_pos] == "T":
            trade_price = float(record[7])
            trade_size = int(record[8])
            bid_price = None
            bid_size = None
            ask_price = None
            ask_size = None
            partition = 'T'
            event = (trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,trade_price, trade_size, bid_price, 
                     bid_size, ask_price, ask_size, partition)
            return event
                   
        # fields specific to record_type 'Q'
        elif record[record_type_pos] == "Q":
            trade_price = None
            trade_size = None
            bid_price = float(record[7])
            bid_size = int(record[8])
            ask_price = float(record[9])
            ask_size = int(record[10])
            partition = 'Q'
            event = (trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,trade_price, trade_size, bid_price, 
                     bid_size, ask_price, ask_size, partition)
            return event
        else:
            raise Exception
            
    
    # capture record in a bad partition if any error occurs
    except Exception as e:
        return (None,None,None,None,None,None,None,None,None,None,None,None,"B")

def parse_json(line:str):
    """
      Function to parse json records
      
    """
    record = json.loads(line)
    try:
        #common fields applicable to both 'Q' and 'T' type records
        trade_dt = datetime.strptime(record['trade_dt'], '%Y-%m-%d')
        rec_type = record['event_type']
        symbol = record['symbol']
        event_time = datetime.timestamp(datetime.strptime(record['event_tm'], '%Y-%m-%d %H:%M:%S.%f'))
        event_seq_num = int(record['event_seq_nb'])
        exchange = record['exchange']
        
        #fields specific to record_type 'T'
        if rec_type == "T":
            trade_price = float(record['price'])
            trade_size = int(record['size'])
            bid_price = None
            bid_size = None
            ask_price = None
            ask_size = None
            partition = 'T'
            event = (trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,trade_price, trade_size, bid_price, 
                     bid_size, ask_price, ask_size, partition)
            return event
                   
        # fields specific to record_type 'Q'
        elif rec_type == "Q":
            trade_price = None
            trade_size = None
            bid_price = float(record['bid_pr'])
            bid_size = int(record['bid_size'])
            ask_price = float(record['ask_pr'])
            ask_size = int(record['ask_size'])
            partition = 'Q'
            event = (trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,trade_price, trade_size, bid_price, 
                     bid_size, ask_price, ask_size, partition)
            return event
        else:
            raise Exception

    # capture record in a bad partition if any error occurs
    except Exception as e:
            return (None,None,None,None,None,None,None,None,None,None,None,None,"B")
    
    
# schema to parse both Q and T type records
common_event = StructType([
    StructField("trade_dt",DateType(), True), 
    StructField("rec_type",StringType()),
    StructField("symbol",StringType()),
    StructField("event_time", FloatType()),
    StructField("event_seq_num", IntegerType()),
    StructField("exchange", StringType()),
    StructField("trade_price", FloatType()),
    StructField("trade_size", IntegerType()),
    StructField("bid_price", FloatType()),
    StructField("bid_size", IntegerType()),
    StructField("ask_price", FloatType()),
    StructField("ask_size", IntegerType()),
    StructField("partition",StringType())
  ])


spark = SparkSession.builder.master('local').\
        appName('app').getOrCreate()

raw_csv = spark.sparkContext.textFile(r"C:\Users\samy8\Desktop\Work Lab\SpringBoard\github\GuidedCapstone\Step2\data\csv\*\NYSE\*.txt")
parsed_csv = raw_csv.map(lambda line: parse_csv(line))
data_csv = spark.createDataFrame(parsed_csv,schema=common_event)

raw_json = spark.sparkContext.textFile(r"C:\Users\samy8\Desktop\Work Lab\SpringBoard\github\GuidedCapstone\Step2\data\json\*\NASDAQ\*.txt")

parsed_json = raw_json.map(lambda line: parse_json(line))
data_json = spark.createDataFrame(parsed_json,schema=common_event)
#print(data_csv.show(1))
#save parquets
data_csv.write.partitionBy("partition").mode("overwrite").csv(r"C:\Users\samy8\Desktop\Work Lab\SpringBoard\github\GuidedCapstone\Step2\out\out1")
data_json.write.partitionBy("partition").mode("overwrite").parquet(r"C:\Users\samy8\Desktop\Work Lab\SpringBoard\github\GuidedCapstone\Step2\out\out2")
print('Spark-Job Execution Complete')