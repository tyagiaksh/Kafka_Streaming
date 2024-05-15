from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# create a session to run spark
spark = SparkSession \
    .builder \
    .getOrCreate()

# a schema for our data in which we will store in kafka
schema =  StructType([
    StructField("Date/Time", StringType(),True),
    StructField("LV_ActivePower",DoubleType(),True),
    StructField("Wind_Speed",DoubleType(),True),
    StructField("Theoretical_Power_Curve", DoubleType(), True),
    StructField("Wind_Direction", DoubleType(), True)
  ])

# reading csv file
df = spark.read\
    .schema(schema)\
    .csv('/home/xs450-akatag/kafka/kafka_2.13-3.7.0/data/read.csv',header="true")\
    # .createOrReplaceTempView("data")

df.show(10)

# now we will write this data in kafka server in json format
df.selectExpr("to_json(struct(*)) AS value").write\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "quickstart-events") \
    .save()

# spark.sql("SELECT *  FROM data").show(5)