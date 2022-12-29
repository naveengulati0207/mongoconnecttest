from pyspark.sql import * 
import pandas as pd
import sys,os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql import Row
        
try:
    
    # Creating Spark Session    & Connector for monodb connect                                                                         
        
        Spark = SparkSession \
            .builder \
            .appName("Read Mongo") \
            .master('local') \
            .config("spark.mongodb.input.uri","mongodb://localhost:27017/mongo_spark_connect.test") \
            .config("spark.mongodb.output.uri","mongodb://localhost:27017/mongo_spark_connect.test") \
            .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .getOrCreate()
        
        # reading data from mongodb we have to define database name and collection name
        ratings = Spark.read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("database","mongo_spark_connect")\
        .option("collection","test")\
        .load()
        
        ratings.show()
        

        # here we are readin csv from local system and write the data on monodb
        SnapCsv = Spark.read.csv('D:/SnapBucketingcsv.csv',header=True,multiLine=True,inferSchema=True)
        SnapCsv.show(50)
        SnapCsv.printSchema()
        
    
        SnapCsv.select("*").write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("database","mongo_spark_connect")\
        .option("collection","test3")\
        .save()
        # studentDf.show()
        # studentDf.select("id","name","marks").write\
        # .format("com.mongodb.spark.sql.DefaultSource")\
        # .option("database","mongo_spark_connect")\
        # .option("collection","test")\
        # .save()
except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, exc_tb.tb_lineno)
    print(exc_type)
    print(fname)
    print(exc_tb.tb_lineno)
    print(e)   