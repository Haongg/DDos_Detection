from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

network_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("src_ip", StringType(), True),
    StructField("dst_ip", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("method", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("request_url", StringType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("response_size", IntegerType(), True)
])
