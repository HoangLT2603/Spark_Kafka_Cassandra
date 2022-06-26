import time
from pyspark.sql import SparkSession
import  os
from pyspark.sql.types import *
from pyspark.sql.functions import *

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 --conf spark.cassandra.connection.host=localhost pyspark-shell'


kafka_topic_name = "new-topic"
kafka_bootstrap_servers = 'localhost:9092'

cassandra_keyspace = "weatherdata"
cassandra_table = "weather_detail"


def get_data_from_kafka():

    weather_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    weather_detail_df_1 = weather_detail_df.selectExpr("CAST(value AS STRING)", "timestamp")

    weather_schema = StructType() \
        .add("CityName", StringType()) \
        .add("Temperature", DoubleType()) \
        .add("Humidity", IntegerType()) \
        .add("CreationTime", StringType())

    weather_detail_df_2 = weather_detail_df_1.select(from_json(col("value"), weather_schema).alias("weather_detail"),
                                                     "timestamp")

    weather_detail_df_3 = weather_detail_df_2.select("weather_detail.*", "timestamp")

    weather_detail_df_4 = weather_detail_df_3.withColumn("CreationDate", col("CreationTime").cast(DateType()))

    weather_detail_df_5 = weather_detail_df_4.select("CityName", "Temperature", "Humidity", "CreationTime",
                                                     "CreationDate")

    weather_detail_df_5.printSchema()

    return  weather_detail_df_5

def save_data_into_cassandra(batchDF, batchID):

    batchDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", cassandra_keyspace) \
        .option("table", cassandra_table) \
        .mode("append")\
        .save()

if __name__ == "__main__":

    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message From Openweathermap Api") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    weather_detail_df_5 = get_data_from_kafka()
    # save_data_into_cassandra(weather_detail_df_5)


    weather_detail_write_stream = weather_detail_df_5.writeStream\
        .trigger(processingTime="5 seconds")\
        .outputMode("update")\
        .option("truncate", "false")\
        .format("console")\
        .start()

    weather_detail_df_5.writeStream\
        .format("csv")\
        .option("path", "/home/hoanglb/PycharmProjects/Spark_Kafka_Project/weather_detail")\
        .option("checkpointLocation", "/home/hoanglb/PycharmProjects/Spark_Kafka_Project/weather_detail_checkpoint") \
        .start()

    weather_detail_df_6 = weather_detail_df_5.select(col("CityName").alias("cityname"),\
                                                     col("Temperature").alias("temperature"),\
                                                     col("Humidity").alias("humidity"),\
                                                     col("CreationTime").alias("creationtime"),\
                                                     col("CreationDate").alias("creationdate"))

    weather_detail_df_6.writeStream \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", "/home/hoanglb/PycharmProjects/Spark_Kafka_Project/cassandra_checkpoint") \
        .foreachBatch(save_data_into_cassandra)\
        .outputMode("update") \
        .start()


    weather_detail_write_stream.awaitTermination()

    print("Stream data processing application completed")