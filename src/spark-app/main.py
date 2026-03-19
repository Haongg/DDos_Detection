import os
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat_ws, to_json, struct

from utils.schema import network_schema
from utils.formatter import clean_data
from config.detection_conf import SparkConfig
from jobs.ddos_detector import ddos_detection_logic


def _normalize_checkpoint_path(path_value):
  path = (path_value or "").strip()
  if not path:
    return ""
  # Accept inputs like "data/spark-job-v2" by normalizing to absolute in-container path.
  if not path.startswith("/"):
    path = f"/{path}"
  return path


def _resolve_checkpoint_run_id():
  configured_run_id = os.getenv("SPARK_CHECKPOINT_RUN_ID", "").strip()
  if configured_run_id:
    return configured_run_id
  return datetime.utcnow().strftime("%Y%m%d-%H%M%S")


def _materialize_checkpoint_path(path_value, run_id):
  path = _normalize_checkpoint_path(path_value)
  if not path:
    return ""
  if "*" in path:
    return path.replace("*", run_id)
  return path


def _resolve_checkpoint_paths():
  run_id = _resolve_checkpoint_run_id()
  base_checkpoint_template = _normalize_checkpoint_path(
    os.getenv("SPARK_CHECKPOINT_DIR", SparkConfig.SETTINGS.get("spark.sql.streaming.checkpointLocation", ""))
  ) or "/data/checkpoints/spark-job-v2-*"
  base_checkpoint = _materialize_checkpoint_path(base_checkpoint_template, run_id)

  stats_checkpoint_template = os.getenv("SPARK_STATS_CHECKPOINT_DIR", "").strip()
  if stats_checkpoint_template:
    stats_checkpoint = _materialize_checkpoint_path(stats_checkpoint_template, run_id)
  else:
    stats_checkpoint = f"{base_checkpoint}-stats"

  os.makedirs(base_checkpoint, exist_ok=True)
  os.makedirs(stats_checkpoint, exist_ok=True)
  print(f"[info] checkpoint_run_id={run_id}")
  print(f"[info] checkpoint_dir={base_checkpoint}")
  print(f"[info] stats_checkpoint_dir={stats_checkpoint}")
  return base_checkpoint, stats_checkpoint


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
  alert_stats_enabled = os.getenv("SPARK_ALERT_STATS_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
  checkpoint_dir, stats_checkpoint_dir = _resolve_checkpoint_paths()

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
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(processingTime=SparkConfig.SETTINGS["spark.sql.streaming.trigger.processingTime"]) \
    .start()

  stats_query = None
  if alert_stats_enabled:
    def _log_alert_stats(batch_df, batch_id):
      if batch_df is None:
        return
      if batch_df.rdd.isEmpty():
        return
      rows = (
        batch_df.groupBy("attack_type")
        .count()
        .orderBy(col("count").desc())
        .collect()
      )
      summary = ", ".join([f"{row['attack_type']}={row['count']}" for row in rows])
      print(f"[alert-stats] batchId={batch_id} {summary}")

    stats_query = df.writeStream \
      .outputMode("update") \
      .foreachBatch(_log_alert_stats) \
      .option("checkpointLocation", stats_checkpoint_dir) \
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
