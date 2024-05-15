from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName("read")\
    .getOrCreate()

# read the data from kafka topics
df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "quickstart-events") \
    .option("startingOffsets", "earliest") \
    .load()
    

# df.printSchema()

# in this we will take values from column convert them to string and adding them to json
json_df = df.selectExpr("cast(value as string) as value")
json_schema =  StructType([
    StructField("Date/Time", StringType(),True),
    StructField("LV_ActivePower",DoubleType(),True),
    StructField("Wind_Speed",DoubleType(),True),
    StructField("Theoretical_Power_Curve", DoubleType(), True),
    StructField("Wind_Direction", DoubleType(), True)
  ])


# now we will apply Schema to JSON value column and expand the value 
json_expand_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*")
query = json_expand_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()\
    .awaitTermination()  



