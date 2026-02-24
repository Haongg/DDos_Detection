from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from utils.schema import network_schema
from utils.formatter import clean_data
from config.detection_conf import SparkConfig
from jobs.ddos_detector import ddos_detection_logic

def consume_from_kafka(spark):
  """
  Hàm này đóng vai trò là 'Connector' giữa Spark và Kafka.
  """
  return spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", SparkConfig.KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", SparkConfig.INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

def main():
  spark = SparkSession.builder \
    .appName(SparkConfig.APP_NAME) \
    .master(SparkConfig.MASTER) \
    .config("spark.driver.memory", SparkConfig.SETTINGS["spark.driver.memory"]) \
    .config("spark.executor.memory", SparkConfig.SETTINGS["spark.executor.memory"]) \
    .config("spark.sql.shuffle.partitions", SparkConfig.SETTINGS["spark.sql.shuffle.partitions"]) \
    .config("spark.streaming.backpressure.enabled", SparkConfig.SETTINGS["spark.streaming.backpressure.enabled"]) \
    .config("spark.sql.streaming.checkpointLocation", SparkConfig.SETTINGS["spark.sql.streaming.checkpointLocation"]) \
    .getOrCreate()
  
  raw_df = consume_from_kafka(spark) \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), network_schema).alias("data")) \
    .select("data.*")

  df = ddos_detection_logic(clean_data(raw_df))

  query = df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()
  
  query.awaitTermination()

if __name__ == "__main__":
  main()