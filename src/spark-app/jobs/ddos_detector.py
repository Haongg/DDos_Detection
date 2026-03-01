from functools import reduce

from pyspark.sql.functions import (
  col,
  concat,
  count,
  approx_count_distinct,
  current_timestamp,
  lit,
  sum as spark_sum,
  when,
  window,
)

WATERMARK_DELAY = "2 minutes"

MAIN_WINDOW_RULES = [
  {"seconds": 5, "slide": 1, "severity": "critical", "rank": 2},
]

FAST_WINDOW_RULES = [
  {"seconds": 2, "slide": 1, "severity": "warning", "rank": 1},
]

MAIN_THRESHOLDS = {
  "http_flood": 300,
  "botnet": 40,
  "search_flood": 160,
  "scanning": 40,
  "slowloris": 30,
  "distributed_heavy_url": 120,
}

# 2s thresholds are intentionally stricter than 5s to reduce false positives.
FAST_THRESHOLDS = {
  "http_flood": 180,
  "scanning": 28,
}

LONG_BASELINE_WINDOW_SECONDS = 60
BASELINE_SLIDE_SECONDS = 60


def _union_all(dfs):
  if not dfs:
    raise ValueError("No DataFrames to union")
  return reduce(lambda left, right: left.unionByName(right), dfs)


def _build_window_alerts(
  df,
  attack_type,
  signal_reason,
  threshold,
  window_rules,
  base_filter=None,
  post_filter=None,
):
  if base_filter is None:
    base_filter = lit(True)

  alerts = []
  source = df.filter(base_filter)

  for rule in window_rules:
    window_seconds = rule["seconds"]

    aggregated = source.groupBy(
      window(col("timestamp"), f"{window_seconds} seconds", f"{rule['slide']} seconds"),
      col("src_ip"),
    ).agg(
      count("*").alias("request_count"),
      approx_count_distinct("request_path").alias("unique_path_count"),
      approx_count_distinct("method").alias("method_variety"),
      spark_sum(when(col("status_category").isin("4xx", "5xx"), 1).otherwise(0)).alias("error_count"),
      spark_sum(when(col("status_code").isin(408, 429, 503, 504), 1).otherwise(0)).alias("timeout_like_count"),
      spark_sum(when(col("response_size") <= 128, 1).otherwise(0)).alias("tiny_response_count"),
    ) \
      .withColumn("rps", col("request_count") / lit(float(window_seconds))) \
      .withColumn(
        "error_ratio",
        when(col("request_count") > 0, col("error_count") / col("request_count")).otherwise(lit(0.0)),
      ) \
      .withColumn(
        "timeout_like_ratio",
        when(col("request_count") > 0, col("timeout_like_count") / col("request_count")).otherwise(lit(0.0)),
      ) \
      .withColumn(
        "tiny_response_ratio",
        when(col("request_count") > 0, col("tiny_response_count") / col("request_count")).otherwise(lit(0.0)),
      ) \
      .filter(col("request_count") >= lit(threshold))

    if post_filter is not None:
      aggregated = aggregated.filter(post_filter)

    alerts.append(
      aggregated.select(
        col("src_ip"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        lit(f"{window_seconds}s").alias("window_size"),
        lit(attack_type).alias("attack_type"),
        lit(rule["severity"]).alias("severity"),
        lit(rule["rank"]).alias("severity_rank"),
        col("request_count"),
        col("rps"),
        col("unique_path_count"),
        col("error_ratio"),
        lit(signal_reason).alias("signal_reason"),
      ),
    )

  return _union_all(alerts)


def detect_http_flood(df):
  common_args = {
    "df": df,
    "attack_type": "http_flood",
    "signal_reason": "high_rps_general_http",
    "base_filter": ~col("url_type").isin("scan_url"),
  }

  main_alerts = _build_window_alerts(
    **common_args,
    threshold=MAIN_THRESHOLDS["http_flood"],
    window_rules=MAIN_WINDOW_RULES,
  )

  fast_alerts = _build_window_alerts(
    **common_args,
    threshold=FAST_THRESHOLDS["http_flood"],
    window_rules=FAST_WINDOW_RULES,
  )

  return main_alerts.unionByName(fast_alerts)


def detect_botnet(df):
  return _build_window_alerts(
    df=df,
    attack_type="botnet",
    signal_reason="distributed_low_and_steady_rps",
    threshold=MAIN_THRESHOLDS["botnet"],
    window_rules=MAIN_WINDOW_RULES,
    base_filter=~col("url_type").isin("scan_url", "search_url"),
    post_filter=col("unique_path_count") >= 3,
  )


def detect_search_flood(df):
  return _build_window_alerts(
    df=df,
    attack_type="search_flood",
    signal_reason="heavy_db_or_login_endpoints",
    threshold=MAIN_THRESHOLDS["search_flood"],
    window_rules=MAIN_WINDOW_RULES,
    base_filter=col("url_type").isin("search_url", "api_report_export_url", "api_login_url"),
  )


def detect_scanning(df):
  common_args = {
    "df": df,
    "attack_type": "scanning",
    "signal_reason": "suspicious_scan_endpoints",
    "base_filter": col("url_type").isin("scan_url"),
    "post_filter": col("error_ratio") >= 0.5,
  }

  main_alerts = _build_window_alerts(
    **common_args,
    threshold=MAIN_THRESHOLDS["scanning"],
    window_rules=MAIN_WINDOW_RULES,
  )

  fast_alerts = _build_window_alerts(
    **common_args,
    threshold=FAST_THRESHOLDS["scanning"],
    window_rules=FAST_WINDOW_RULES,
  )

  return main_alerts.unionByName(fast_alerts)


def detect_slowloris(df):
  return _build_window_alerts(
    df=df,
    attack_type="slowloris",
    signal_reason="slow_connection_behavior",
    threshold=MAIN_THRESHOLDS["slowloris"],
    window_rules=MAIN_WINDOW_RULES,
    post_filter=(
      (col("timeout_like_ratio") >= 0.30)
      & (col("tiny_response_ratio") >= 0.60)
      & (col("method_variety") <= 2)
      & (col("unique_path_count") <= 3)
    ),
  )


def detect_distributed_heavy_url(df):
  # 60s tumbling window avoids Expand x12 caused by 60s/5s sliding windows.
  min_60s_count = float(MAIN_THRESHOLDS["distributed_heavy_url"] * (LONG_BASELINE_WINDOW_SECONDS / 5))

  long_path = df.groupBy(
    window(
      col("timestamp"),
      f"{LONG_BASELINE_WINDOW_SECONDS} seconds",
      f"{BASELINE_SLIDE_SECONDS} seconds",
    ),
    col("request_path"),
  ).agg(
    count("*").alias("path_request_count_60s"),
    approx_count_distinct("src_ip").alias("unique_src_ip"),
    spark_sum(when(col("status_category").isin("4xx", "5xx"), 1).otherwise(0)).alias("path_error_count"),
  ).withColumn(
    "path_error_ratio",
    when(col("path_request_count_60s") > 0, col("path_error_count") / col("path_request_count_60s")).otherwise(lit(0.0)),
  ).filter(
    (col("path_request_count_60s") >= lit(min_60s_count))
    & (col("unique_src_ip") >= lit(20))
  )

  return long_path.select(
    lit("__distributed__").alias("src_ip"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    lit("60s").alias("window_size"),
    lit("distributed_heavy_url").alias("attack_type"),
    lit("warning").alias("severity"),
    lit(2).alias("severity_rank"),
    col("path_request_count_60s").alias("request_count"),
    (col("path_request_count_60s") / lit(float(LONG_BASELINE_WINDOW_SECONDS))).alias("rps"),
    col("unique_src_ip").alias("unique_path_count"),
    col("path_error_ratio").alias("error_ratio"),
    concat(lit("distributed_spike_on_path:"), col("request_path")).alias("signal_reason"),
  )


def ddos_detection_logic(df):
  watermarked_df = df.withWatermark("timestamp", WATERMARK_DELAY)

  detector_outputs = [
    detect_http_flood(watermarked_df),
    detect_botnet(watermarked_df),
    detect_search_flood(watermarked_df),
    detect_scanning(watermarked_df),
    detect_slowloris(watermarked_df),
    detect_distributed_heavy_url(watermarked_df),
  ]

  merged_alerts = _union_all(detector_outputs)

  return merged_alerts.withColumn("detected_at", current_timestamp())
