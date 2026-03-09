import os


SEVERITY_RANK = {
    "warning": 1,
    "critical": 2,
}


class AlertConfig:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "ddos-alerts")
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "alert-service-group")
    KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
    KAFKA_POLL_TIMEOUT = float(os.getenv("KAFKA_POLL_TIMEOUT", "1.0"))
    KAFKA_SESSION_TIMEOUT_MS = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "45000"))
    KAFKA_HEARTBEAT_INTERVAL_MS = int(os.getenv("KAFKA_HEARTBEAT_INTERVAL_MS", "15000"))
    KAFKA_MAX_POLL_INTERVAL_MS = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000"))
    KAFKA_REQUEST_TIMEOUT_MS = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "90000"))
    KAFKA_RETRY_BACKOFF_MS = int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "1000"))
    KAFKA_RECONNECT_BACKOFF_MS = int(os.getenv("KAFKA_RECONNECT_BACKOFF_MS", "1000"))
    KAFKA_RECONNECT_BACKOFF_MAX_MS = int(os.getenv("KAFKA_RECONNECT_BACKOFF_MAX_MS", "10000"))

    REDIS_HOST = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))
    ALERT_DEDUP_TTL_SECONDS = int(os.getenv("ALERT_DEDUP_TTL_SECONDS", "300"))
    REDIS_SOCKET_TIMEOUT_SECONDS = float(os.getenv("REDIS_SOCKET_TIMEOUT_SECONDS", "2.0"))
    REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS = float(
        os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS", "2.0")
    )

    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    ALERT_MIN_SEVERITY = os.getenv("ALERT_MIN_SEVERITY", "critical").lower()
    ALERT_STATS_INTERVAL_SECONDS = int(os.getenv("ALERT_STATS_INTERVAL_SECONDS", "30"))

    CONSUMER_CONFIG = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
        "enable.auto.commit": False,
        "session.timeout.ms": KAFKA_SESSION_TIMEOUT_MS,
        "heartbeat.interval.ms": KAFKA_HEARTBEAT_INTERVAL_MS,
        "max.poll.interval.ms": KAFKA_MAX_POLL_INTERVAL_MS,
        "request.timeout.ms": KAFKA_REQUEST_TIMEOUT_MS,
        "retry.backoff.ms": KAFKA_RETRY_BACKOFF_MS,
        "reconnect.backoff.ms": KAFKA_RECONNECT_BACKOFF_MS,
        "reconnect.backoff.max.ms": KAFKA_RECONNECT_BACKOFF_MAX_MS,
    }


def should_notify(severity):
    current_rank = SEVERITY_RANK.get((severity or "").lower(), 0)
    min_rank = SEVERITY_RANK.get(AlertConfig.ALERT_MIN_SEVERITY, 2)
    return current_rank >= min_rank
