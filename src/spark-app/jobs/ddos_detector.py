import os
from functools import reduce

from pyspark.sql.functions import (
  approx_count_distinct,
  col,
  concat,
  count,
  current_timestamp,
  lit,
  sum as spark_sum,
  when,
  window,
)

def _flag(name, default):
  return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


WATERMARK_DELAY = os.getenv("SPARK_WATERMARK_DELAY", "45 seconds")
ENABLE_FAST_WINDOW_RULES = _flag("SPARK_ENABLE_FAST_WINDOW_RULES", "false")
ENABLE_DISTRIBUTED_HEAVY_URL_RULE = _flag("SPARK_ENABLE_DISTRIBUTED_HEAVY_URL_RULE", "true")

MAIN_WINDOW_RULES = [
  {"seconds": 5, "slide": 1, "severity": "critical", "rank": 2},
]

FAST_WINDOW_RULES = [
  {"seconds": 2, "slide": 1, "severity": "warning", "rank": 1},
]

MAIN_THRESHOLDS = {
  "http_flood": 150,
  "botnet": 40,
  "search_flood": 120,
  "scanning": 40,
  "slowloris": 30,
  "distributed_heavy_url": 120,
}

# 2s thresholds are intentionally stricter than 5s to reduce false positives.
FAST_THRESHOLDS = {
  "http_flood": 100,
  "scanning": 28,
}

LONG_BASELINE_WINDOW_SECONDS = 60
BASELINE_SLIDE_SECONDS = 60


def _union_all(dfs):
  if not dfs:
    raise ValueError("No DataFrames to union")
  return reduce(lambda left, right: left.unionByName(right), dfs)


def _safe_ratio(numerator_col, denominator_col):
  return when(denominator_col > 0, numerator_col / denominator_col).otherwise(lit(0.0))


def _aggregate_src_ip_window(df, window_seconds, slide_seconds):
  scan_only = col("url_type").isin("scan_url")
  non_scan = ~scan_only
  non_scan_non_search = ~col("url_type").isin("scan_url", "search_url")
  search_focus = col("url_type").isin("search_url", "api_report_export_url", "api_login_url")

  return df.groupBy(
    window(col("timestamp"), f"{window_seconds} seconds", f"{slide_seconds} seconds"),
    col("src_ip"),
  ).agg(
    count("*").alias("request_count_all"),
    spark_sum(when(non_scan, 1).otherwise(0)).alias("request_count_non_scan"),
    spark_sum(when(non_scan_non_search, 1).otherwise(0)).alias("request_count_non_scan_non_search"),
    spark_sum(when(search_focus, 1).otherwise(0)).alias("request_count_search_focus"),
    spark_sum(when(scan_only, 1).otherwise(0)).alias("request_count_scan_only"),
    approx_count_distinct("request_path").alias("unique_path_count_all"),
    approx_count_distinct(when(non_scan, col("request_path"))).alias("unique_path_count_non_scan"),
    approx_count_distinct(when(non_scan_non_search, col("request_path"))).alias("unique_path_count_non_scan_non_search"),
    approx_count_distinct(when(search_focus, col("request_path"))).alias("unique_path_count_search_focus"),
    approx_count_distinct(when(scan_only, col("request_path"))).alias("unique_path_count_scan_only"),
    approx_count_distinct("method").alias("method_variety"),
    spark_sum(when(col("status_category").isin("4xx", "5xx"), 1).otherwise(0)).alias("error_count_all"),
    spark_sum(when(non_scan & col("status_category").isin("4xx", "5xx"), 1).otherwise(0)).alias("error_count_non_scan"),
    spark_sum(
      when(non_scan_non_search & col("status_category").isin("4xx", "5xx"), 1).otherwise(0),
    ).alias("error_count_non_scan_non_search"),
    spark_sum(when(search_focus & col("status_category").isin("4xx", "5xx"), 1).otherwise(0)).alias("error_count_search_focus"),
    spark_sum(when(scan_only & col("status_category").isin("4xx", "5xx"), 1).otherwise(0)).alias("error_count_scan_only"),
    spark_sum(when(col("status_code").isin(408, 429, 503, 504), 1).otherwise(0)).alias("timeout_like_count_all"),
    spark_sum(when(col("response_size") <= 128, 1).otherwise(0)).alias("tiny_response_count_all"),
  )


def _build_alerts_from_aggregate(
  aggregated_df,
  request_count_col,
  unique_path_count_col,
  error_count_col,
  threshold,
  window_seconds,
  attack_type,
  severity,
  severity_rank,
  signal_reason,
  post_filter=None,
):
  alerts = aggregated_df \
    .withColumn("request_count", col(request_count_col)) \
    .withColumn("unique_path_count", col(unique_path_count_col)) \
    .withColumn("rps", col(request_count_col) / lit(float(window_seconds))) \
    .withColumn(
      "error_ratio",
      _safe_ratio(col(error_count_col), col(request_count_col)),
    ) \
    .filter(col("request_count") >= lit(threshold))

  if post_filter is not None:
    alerts = alerts.filter(post_filter)

  return alerts.select(
    col("src_ip"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    lit(f"{window_seconds}s").alias("window_size"),
    lit(attack_type).alias("attack_type"),
    lit(severity).alias("severity"),
    lit(severity_rank).alias("severity_rank"),
    col("request_count"),
    col("rps"),
    col("unique_path_count"),
    col("error_ratio"),
    lit(signal_reason).alias("signal_reason"),
  )


def _detect_on_5s_aggregate(agg_5s):
  rule = MAIN_WINDOW_RULES[0]

  http_flood = _build_alerts_from_aggregate(
    aggregated_df=agg_5s,
    request_count_col="request_count_non_scan",
    unique_path_count_col="unique_path_count_non_scan",
    error_count_col="error_count_non_scan",
    threshold=MAIN_THRESHOLDS["http_flood"],
    window_seconds=rule["seconds"],
    attack_type="http_flood",
    severity=rule["severity"],
    severity_rank=rule["rank"],
    signal_reason="high_rps_general_http",
  )

  botnet = _build_alerts_from_aggregate(
    aggregated_df=agg_5s,
    request_count_col="request_count_non_scan_non_search",
    unique_path_count_col="unique_path_count_non_scan_non_search",
    error_count_col="error_count_non_scan_non_search",
    threshold=MAIN_THRESHOLDS["botnet"],
    window_seconds=rule["seconds"],
    attack_type="botnet",
    severity=rule["severity"],
    severity_rank=rule["rank"],
    signal_reason="distributed_low_and_steady_rps",
    post_filter=col("unique_path_count_non_scan_non_search") >= 3,
  )

  search_flood = _build_alerts_from_aggregate(
    aggregated_df=agg_5s,
    request_count_col="request_count_search_focus",
    unique_path_count_col="unique_path_count_search_focus",
    error_count_col="error_count_search_focus",
    threshold=MAIN_THRESHOLDS["search_flood"],
    window_seconds=rule["seconds"],
    attack_type="search_flood",
    severity=rule["severity"],
    severity_rank=rule["rank"],
    signal_reason="heavy_db_or_login_endpoints",
  )

  scanning = _build_alerts_from_aggregate(
    aggregated_df=agg_5s,
    request_count_col="request_count_scan_only",
    unique_path_count_col="unique_path_count_scan_only",
    error_count_col="error_count_scan_only",
    threshold=MAIN_THRESHOLDS["scanning"],
    window_seconds=rule["seconds"],
    attack_type="scanning",
    severity=rule["severity"],
    severity_rank=rule["rank"],
    signal_reason="suspicious_scan_endpoints",
    post_filter=_safe_ratio(col("error_count_scan_only"), col("request_count_scan_only")) >= 0.5,
  )

  slowloris = _build_alerts_from_aggregate(
    aggregated_df=agg_5s,
    request_count_col="request_count_all",
    unique_path_count_col="unique_path_count_all",
    error_count_col="error_count_all",
    threshold=MAIN_THRESHOLDS["slowloris"],
    window_seconds=rule["seconds"],
    attack_type="slowloris",
    severity=rule["severity"],
    severity_rank=rule["rank"],
    signal_reason="slow_connection_behavior",
    post_filter=(
      (_safe_ratio(col("timeout_like_count_all"), col("request_count_all")) >= 0.30)
      & (_safe_ratio(col("tiny_response_count_all"), col("request_count_all")) >= 0.60)
      & (col("method_variety") <= 2)
      & (col("unique_path_count_all") <= 3)
    ),
  )

  return [http_flood, botnet, search_flood, scanning, slowloris]


def _detect_on_2s_aggregate(agg_2s):
  rule = FAST_WINDOW_RULES[0]

  http_flood_fast = _build_alerts_from_aggregate(
    aggregated_df=agg_2s,
    request_count_col="request_count_non_scan",
    unique_path_count_col="unique_path_count_non_scan",
    error_count_col="error_count_non_scan",
    threshold=FAST_THRESHOLDS["http_flood"],
    window_seconds=rule["seconds"],
    attack_type="http_flood",
    severity=rule["severity"],
    severity_rank=rule["rank"],
    signal_reason="high_rps_general_http",
  )

  scanning_fast = _build_alerts_from_aggregate(
    aggregated_df=agg_2s,
    request_count_col="request_count_scan_only",
    unique_path_count_col="unique_path_count_scan_only",
    error_count_col="error_count_scan_only",
    threshold=FAST_THRESHOLDS["scanning"],
    window_seconds=rule["seconds"],
    attack_type="scanning",
    severity=rule["severity"],
    severity_rank=rule["rank"],
    signal_reason="suspicious_scan_endpoints",
    post_filter=_safe_ratio(col("error_count_scan_only"), col("request_count_scan_only")) >= 0.5,
  )

  return [http_flood_fast, scanning_fast]


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
    _safe_ratio(col("path_error_count"), col("path_request_count_60s")),
  ).filter(
    (col("path_request_count_60s") >= lit(min_60s_count))
    & (col("unique_src_ip") >= lit(20))
  )

  return long_path.select(
    lit(None).cast("string").alias("src_ip"),
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

  agg_5s = _aggregate_src_ip_window(
    df=watermarked_df,
    window_seconds=MAIN_WINDOW_RULES[0]["seconds"],
    slide_seconds=MAIN_WINDOW_RULES[0]["slide"],
  )
  detector_outputs = _detect_on_5s_aggregate(agg_5s)

  if ENABLE_FAST_WINDOW_RULES:
    agg_2s = _aggregate_src_ip_window(
      df=watermarked_df,
      window_seconds=FAST_WINDOW_RULES[0]["seconds"],
      slide_seconds=FAST_WINDOW_RULES[0]["slide"],
    )
    detector_outputs += _detect_on_2s_aggregate(agg_2s)

  if ENABLE_DISTRIBUTED_HEAVY_URL_RULE:
    detector_outputs.append(detect_distributed_heavy_url(watermarked_df))

  merged_alerts = _union_all(detector_outputs)

  return merged_alerts.withColumn("detected_at", current_timestamp())
