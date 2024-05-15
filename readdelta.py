from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import *

spark = SparkSession \
    .builder \
    .appName("DeltaTutorial") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()\
    
 

read_df = spark.read\
    .format("delta")\
    .load("/home/xs450-akatag/kafka/kafka_2.13-3.7.0/deltatable/location")
# .createOrReplaceTempView("abc")

'''************************************************************ANALYSIS TASK************************************************************'''


# read delta table
read_df.show()
read_df.printSchema()
# root
#  |-- Date/Time: string (nullable = true)
#  |-- signal_date: string (nullable = true)
#  |-- signal_ts: string (nullable = true)
#  |-- create_date: string (nullable = true)
#  |-- create_ts: timestamp (nullable = true)
#  |-- signals: map (nullable = true)
#  |    |-- key: string
#  |    |-- value: double (valueContainsNull = true)




# below will be used to call the map in delta table
# spark.sql("SELECT signals.LV_ActivePower FROM abc").show()


# Calculate number of distinct `signal_ts` datapoints per day
distinct_signal=read_df.groupBy("signal_date").agg(countDistinct("signal_ts").alias("distinct_signal_ts"))
distinct_signal.show()


# Add a column in the dataframe of above named as `generation_indicator` and it will have value as
df=read_df.withColumn("generation_indicator",when(col("signals.LV_ActivePower")<200,"Low")\
                    .when((col("signals.LV_ActivePower")>=200) & (col("signals.LV_ActivePower")<600),"Medium")\
                    .when((col("signals.LV_ActivePower")>=600) & (col("signals.LV_ActivePower")<1000),"High")\
                    .otherwise("Exceptional"))

# df.printSchema()
df.show()


# Create dataframe from JSON data
json_df = [
    {"sig_name": "LV_ActivePower", "sig_mapping_name": "LV_ActivePower_average"},
    {"sig_name": "Wind_Speed", "sig_mapping_name": "Wind_Speed_average"},
    {"sig_name": "Theoretical_Power_Curve", "sig_mapping_name": "Theoretical_Power_Curve_average"},
    {"sig_name": "Wind_Direction", "sig_mapping_name": "Wind Direction_average"}
]
 
new_df = spark.createDataFrame([Row(**x) for x in json_df])
new_df.show()

# perform brodcast join between 4th and 5th 
broadcast_df=df.join(broadcast(new_df),df["generation_indicator"] == new_df["sig_mapping_name"],"left_outer")

# broadcast_df=broadcast_df.withColumn("generation_indicator",broadcast_df.sig_mapping_name)
# broadcast_df = broadcast_df.drop("sig_name", "sig_mapping_name")
broadcast_df.show()

