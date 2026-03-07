import os

class SparkConfig:
    APP_NAME = "DDoS-Detection-Engine"
    MASTER = "local[*]"

    SETTINGS = {
        "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "1g"),
        "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "1g"),
        "spark.sql.shuffle.partitions": os.getenv("SHUFFLE_PARTITIONS", "4"),
        "spark.streaming.backpressure.enabled": "true",
        "spark.sql.streaming.checkpointLocation": os.getenv("SPARK_CHECKPOINT_DIR"),
        "spark.sql.streaming.failOnDataLoss": os.getenv("SPARK_FAIL_ON_DATA_LOSS", "false"),
        "spark.sql.streaming.stateStore.maintenanceInterval": os.getenv("SPARK_STATE_MAINTENANCE_INTERVAL", "30s"),
        "spark.sql.streaming.multipleWatermarkPolicy": os.getenv("SPARK_MULTIPLE_WATERMARK_POLICY", "min"),
        "spark.sql.streaming.minBatchesToRetain": os.getenv("SPARK_MIN_BATCHES_TO_RETAIN", "50"),
        "spark.sql.streaming.maxOffsetsPerTrigger": os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "1000"),
        "spark.sql.streaming.trigger.processingTime": os.getenv("SPARK_TRIGGER_INTERVAL", "2 seconds"),
        "spark.sql.streaming.statefulOperator.checkCorrectness.enabled": os.getenv("SPARK_CHECK_CORRECTNESS_ENABLED", "false"),
    }

    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    INPUT_TOPIC = os.getenv("INPUT_TOPIC", "network-traffic")
    OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "ddos-alerts")

    DDOS_THRESHOLD = int(os.getenv("DDOS_THRESHOLD", "1000"))
