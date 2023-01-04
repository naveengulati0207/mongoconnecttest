import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType
from pyspark.sql import functions
from pyspark.sql.functions import *
from pyspark.sql.functions import current_date
from pyspark.sql.types import *
from pandas.conftest import keep
# --------------------- 1. Readin Data From Csv ------------------------------

Spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example: Reading CSV file without mentioning schema") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


Rawdata = Spark.read.csv(r'D:/Snapshot Input.csv',header=True,multiLine=True,inferSchema=True)
Rawdata.show()

# Rawdata = Rawdata.withColumn('Date', Rawdata['Date'].cast(DateType()))
#
# Rawdata.show()
Rawdata = Rawdata \
            .withColumn("Date",to_date(Rawdata.Date,"dd-MM-yyyy"))
# cnt = Rawdata.count()
# print(cnt)


# # --------------------------- 2. Get Minimum  and maximum date ------------------------
#
# Rawdata.printSchema()

mindataframe = Rawdata.select(min('Date'))
mindataframe.show()
#

mindataframe = mindataframe \
.withColumn('min_date', to_date('min(Date)'))

# mindataframe.show()

from pyspark.sql import functions as F


dateRange = mindataframe.withColumn('link_date', F.explode(F.expr('sequence(min_date, current_date(), interval 1 day ) as Date' ) ))
dateRange.show()

# ---------------------- 3. Dropping Extra Column from date range ----------------

dateRange = dateRange.drop('min(Date)','min_date')
Rawdata = Rawdata.drop('_c0')




# --------------------------- 3. Using Group by -------------------------------

groupcol = ['Branch_Code','lot_code']
groupingdata = Rawdata.groupby(groupcol).agg(functions.sum('net_qty'),functions.min('Date'),functions.max('Date'))
print('grouping dat--------------')
groupingdata.show()
groupingdata = groupingdata.withColumnRenamed('min(Date)', 'min').withColumnRenamed('max(Date)', 'max').withColumnRenamed('sum(net_qty)', 'sum_net_qty').withColumnRenamed('Branch_Code', 'Branch_Code_x')
print('------------  roupin data ----------------------')
groupingdata.show()

# ------------------------------ 4. Add Flag in Date Range -------------------

dateRange = dateRange.select('*',lit("1").alias("Key"))
# dateRange.show(truncate=False)


# ------------------------- 5. Joining Dataframe of daterange and grouped data ---------------


groupingdata = groupingdata.select('*',lit("1").alias("Key"))
# groupingdata.show(truncate=False)


join_result = dateRange.join(groupingdata,on="Key",how="outer")
# join_result.show()



# # # ---------------------- 6. Filtering The Columns -----------------


join_result =  join_result.filter(join_result.link_date >= join_result.min)
# join_result.show()
#
# cnt = join_result.count()
# print(cnt)


join_result = join_result.withColumn("Flag",when((join_result.sum_net_qty == 0)  & (join_result.link_date > join_result.max), 0).otherwise(1))
# join_result.show()


# -------------------------- 7. filter that result where flag =1 ---------------------------

join_result =  join_result.filter(join_result.Flag  == 1)
# join_result.show()


# ------------------------  8. merge with first table outer join-------------------------------

merge_Result = join_result.join(Rawdata,on="lot_code",how="outer")
# merge_Result.show()


# ----------------------- 9. filtering  of dates ------------------------------

merge_Result = merge_Result.withColumn("qty",when((merge_Result.link_date == merge_Result.Date), merge_Result.net_qty).otherwise(0))
# merge_Result.show()



# ------------------------- 10. cumsum of qty -----------------------------------

from pyspark.sql import Window

windowval = (Window.partitionBy('Branch_Code_x','lot_code').orderBy('link_date'))
merge_Result = merge_Result.withColumn('cum_sum', F.sum('qty').over(windowval))
# merge_Result.show(200)



# ------------------------------ 11. filter on sum_net_qty and link date --------------------------------


merge_Result = merge_Result.withColumn("Flag1",when((merge_Result.sum_net_qty == 0)  & (merge_Result.link_date > merge_Result.max), 0).otherwise(1))
# merge_Result.show()


merge_Result =  merge_Result.filter(merge_Result.Flag  == 1)
# merge_Result.show()

  
# cnt = merge_Result.count()
# print(cnt)



# -------------------------- 12. Dropping Extra columns ------------------------------

final_Result = merge_Result.drop('Key','sum_net_qty','min','max','Flag','Branch_Code','net_qty','Flag1','Date','qty')
# final_Result.show()


# -------------------------- 13. Dropping Duplicates ----------------------------------



final_Result = final_Result.distinct()

final_Result.show(300)

res = final_Result.count()
print('filtering result-----------',res)

# final_Result.write.csv("Naveen_Sanptest_pyspark.csv",header=True)

# final_Result.show()



# -------------------------- 14. dropping extra rows ----------------------



# final_Result = final_Result.withColumn('New_col',
#                      when(final_Result.link_date == final_Result.link_date, "True")
#                      .when(final_Result.cum_sum != final_Result.cum_sum, "True")
#                      .otherwise('False'))

# final_Result = final_Result.withColumn('New_col',
#                      final_Result.link_date)
#
# final_Result.show()                     
# from pyspark.sql import functions as f
# # df = df.withColumn('equals_earlier', F.col('ID') == F.lag('ID').over(w))
#
# w = W.orderBy(F.desc('Branch_Code_x'))
#
# final_Result = final_Result.withColumn('equals_earlier', F.col('link_date') == F.lag('link_date').over(w))
# final_Result.show()
#
# cnt = final_Result.count()
# print(cnt)
# final_Result = final_Result.withColumn("Flag",when((final_Result.link_date == 0)  & (final_Result.link_date > merge_Result.max), 0).otherwise(1))

# from pyspark.sql import Window
# w = Window.partitionBy('link_date')
# final_Result = final_Result.withColumn('maxB', f.max('cum_sum').over(w))\
#     .where(f.col('cum_sum') == f.col('maxB'))
    
    
# final_Result =  final_Result.drop_duplicates(['link_date','cum_sum'])   



# w = Window.partitionBy('link_date').orderBy(desc('link_date'))
# final_Result = final_Result.withColumn('Rank',dense_rank().over(w))

# rdd_df = final_Result.rdd.zipWithIndex()
# final_Result = rdd_df.toDF()
# final_Result.show()
#
#

# from pyspark.sql import Window
# the window is necessary here because row_number is a windowing function
# that means you can have row_number run over some amount of your data
# we'll be currently running it over the sorted by column1 data, row per row - our window will be of size 2 (rows),
# the whole dataframe that is.
# window = Window.orderBy(F.col('link_Date'))
# final_Result = final_Result.withColumn('row_number', F.row_number().over(window))
# final_Result.show()
# from pyspark.sql.functions import *
# from pyspark import SparkContext
# from pyspark.sql import SQLContext
#
#
# sc = SparkContext('local', 'Spark SQL') 
# sqlContext = SQLContext(sc)
# final_Result.registerTempTable('table_view')
# final_Result=sqlContext.sql(
#     'select ID, Hit, sum(Hit) over (partition by ID) as sum_Hit from table_view'
# )
# final_Result.show()
#
# w = Window.orderBy("Branch_Code_x").rowsBetween(1,1)
# final_Result = final_Result.withColumn("PrevStopName", F.lead("cum_sum").over(w))
#
# final_Result = final_Result.filter((final_Result.cum_sum == final_Result.PrevStopName)).drop(final_Result.PrevStopName)
#
# res = final_Result.count()
# print('filtering result-----------',res)

# w = Window.partitionBy('link_date').orderBy('cum_sum')
# final_Result = final_Result.withColumn("ranking_ntile_rank",dense_rank().over(w))
# res = final_Result.count()
# print('filtering result-----------',res)
# #
# final_Result = final_Result.filter((final_Result.ranking_ntile_rank !=2)).drop(final_Result.ranking_ntile_rank)
#
# res = final_Result.count()
# print('filtering result-----------',res)

# final_Result.write.csv("Naveen_Sanptest_pyspark_test17.csv",header=True)

# dateRange.show()
# Rawdata.show()