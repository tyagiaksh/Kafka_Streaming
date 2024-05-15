from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import *

spark = SparkSession \
    .builder \
    .appName("DeltaTutorial") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()
 
schema = StructType([
    StructField("Date/Time", StringType(),True),
    StructField("LV_ActivePower",DoubleType(),True),
    StructField("Wind_Speed",DoubleType(),True),
    StructField("Theoretical_Power_Curve", DoubleType(), True),
    StructField("Wind_Direction", DoubleType(), True)
  ])

dfr = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "quickstart-events") \
    .option("startingOffsets", "earliest") \
    .load()

# dfr.printSchema()
 

constr = dfr.select(from_json(col("value").cast("string"), schema).alias("value")).select("value.*")
 
constr.writeStream\
    .format("console")\
    .outputMode("append")\
    .start()
 
# we will create deltatable with below specifications
delta_dfr = constr \
    .withColumn("signal_date" , col("Date/Time").substr(0 , 10)) \
    .withColumn("signal_ts" , col("Date/Time")) \
    .withColumn("create_date", current_date().substr(0 , 10)) \
    .withColumn("create_ts" , current_timestamp()) \
    .withColumn("signals" , create_map(lit("LV_ActivePower"),col("LV_ActivePower"),
        lit("Wind_Speed"),col("Wind_Speed"),
        lit("Theoretical_Power_Curve"),col("Theoretical_Power_Curve"),
        lit("Wind_Direction"),col("Wind_Direction"),
        )
    ).drop("DateTime","LV_ActivePower","Wind_Speed","Theoretical_Power_Curve","Wind_Direction")
# delta_dfr.printSchema()
 
query = delta_dfr.writeStream\
        .format("delta") \
        .outputMode("append")\
        .option("mergeSchema", "true") \
        .option("checkpointLocation", "/home/xs450-akatag/kafka/kafka_2.13-3.7.0/check") \
        .start("/home/xs450-akatag/kafka/kafka_2.13-3.7.0/deltatable/location")


query.awaitTermination()