import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat_ws, to_json, struct

from utils.schema import network_schema
from utils.formatter import clean_data
from config.detection_conf import SparkConfig
from jobs.ddos_detector import ddos_detection_logic


def consume_from_kafka(spark):
  return spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", SparkConfig.KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", SparkConfig.INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", SparkConfig.SETTINGS["spark.sql.streaming.failOnDataLoss"]) \
    .option("maxOffsetsPerTrigger", SparkConfig.SETTINGS["spark.sql.streaming.maxOffsetsPerTrigger"]) \
    .load()


def main():
  progress_log_interval_seconds = int(os.getenv("SPARK_PROGRESS_LOG_INTERVAL_SECONDS", "15"))

  spark = SparkSession.builder \
    .appName(SparkConfig.APP_NAME) \
    .master(SparkConfig.MASTER) \
    .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true") \
    .config("spark.driver.memory", SparkConfig.SETTINGS["spark.driver.memory"]) \
    .config("spark.executor.memory", SparkConfig.SETTINGS["spark.executor.memory"]) \
    .config("spark.sql.shuffle.partitions", SparkConfig.SETTINGS["spark.sql.shuffle.partitions"]) \
    .config("spark.streaming.backpressure.enabled", SparkConfig.SETTINGS["spark.streaming.backpressure.enabled"]) \
    .config("spark.sql.streaming.stateStore.maintenanceInterval", SparkConfig.SETTINGS["spark.sql.streaming.stateStore.maintenanceInterval"]) \
    .config("spark.sql.streaming.multipleWatermarkPolicy", SparkConfig.SETTINGS["spark.sql.streaming.multipleWatermarkPolicy"]) \
    .config("spark.sql.streaming.minBatchesToRetain", SparkConfig.SETTINGS["spark.sql.streaming.minBatchesToRetain"]) \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", SparkConfig.SETTINGS["spark.sql.streaming.statefulOperator.checkCorrectness.enabled"]) \
    .getOrCreate()

  raw_df = consume_from_kafka(spark) \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), network_schema).alias("data")) \
    .select("data.*")

  df = ddos_detection_logic(clean_data(raw_df))
  kafka_df = df.select(
    concat_ws(
      "|",
      col("src_ip"),
      col("attack_type"),
      col("window_end").cast("string"),
      col("window_size"),
    ).alias("key"),
    to_json(struct("*")).alias("value"),
  )

  query = kafka_df.writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", SparkConfig.KAFKA_BOOTSTRAP_SERVERS) \
    .option("topic", SparkConfig.OUTPUT_TOPIC) \
    .option("checkpointLocation",SparkConfig.SETTINGS["spark.sql.streaming.checkpointLocation"]) \
    .trigger(processingTime=SparkConfig.SETTINGS["spark.sql.streaming.trigger.processingTime"]) \
    .start()

  while query.isActive:
    query.awaitTermination(progress_log_interval_seconds)
    progress = query.lastProgress
    if progress:
      duration = progress.get("durationMs", {})
      print(
        "[progress] batchId={batch} numInputRows={rows} "
        "inputRowsPerSecond={in_rps} processedRowsPerSecond={out_rps} "
        "addBatchMs={add_batch} triggerMs={trigger}".format(
          batch=progress.get("batchId"),
          rows=progress.get("numInputRows"),
          in_rps=progress.get("inputRowsPerSecond"),
          out_rps=progress.get("processedRowsPerSecond"),
          add_batch=duration.get("addBatch"),
          trigger=duration.get("triggerExecution"),
        )
      )
    else:
      print("[progress] Waiting for first micro-batch...")
    time.sleep(0.2)


if __name__ == "__main__":
  main()
