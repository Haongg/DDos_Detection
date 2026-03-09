import json
import time

from confluent_kafka import Consumer

from config import AlertConfig, should_notify
from deduplicator import AlertDeduplicator
from notifier import TelegramNotifier


def _decode_message(msg):
    try:
        raw_value = msg.value()
        if raw_value is None:
            return None
        return json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        print(f"[warn] Drop invalid alert payload: {exc}")
        return None


def _message_key(msg):
    raw_key = msg.key()
    if raw_key is None:
        return ""
    try:
        return raw_key.decode("utf-8")
    except UnicodeDecodeError:
        return ""


def _fallback_key(alert):
    return "|".join(
        [
            str(alert.get("src_ip", "unknown")),
            str(alert.get("attack_type", "unknown")),
            str(alert.get("window_end", "unknown")),
            str(alert.get("window_size", "unknown")),
        ]
    )


def _commit_message(consumer, msg):
    try:
        consumer.commit(message=msg, asynchronous=True)
    except Exception as exc:
        print(f"[warn] Commit failed: {exc}")


def _on_assign(_, partitions):
    assignment = ", ".join([f"{p.topic}[{p.partition}]" for p in partitions]) or "(none)"
    print(f"[info] Assigned partitions: {assignment}")


def _on_revoke(_, partitions):
    revoked = ", ".join([f"{p.topic}[{p.partition}]" for p in partitions]) or "(none)"
    print(f"[warn] Revoked partitions: {revoked}")


def _log_stats(stats, last_message_at):
    idle_seconds = int(time.time() - last_message_at) if last_message_at else -1
    print(
        "[stats] consumed={consumed} sent={sent} skipped_severity={skipped_severity} "
        "duplicates={duplicates} invalid={invalid} consumer_errors={consumer_errors} "
        "send_failures={send_failures} idle_seconds={idle}".format(
            consumed=stats["consumed"],
            sent=stats["sent"],
            skipped_severity=stats["skipped_severity"],
            duplicates=stats["duplicates"],
            invalid=stats["invalid"],
            consumer_errors=stats["consumer_errors"],
            send_failures=stats["send_failures"],
            idle=idle_seconds,
        )
    )


def main():
    consumer = Consumer(AlertConfig.CONSUMER_CONFIG)
    consumer.subscribe([AlertConfig.OUTPUT_TOPIC], on_assign=_on_assign, on_revoke=_on_revoke)

    deduplicator = AlertDeduplicator(
        host=AlertConfig.REDIS_HOST,
        port=AlertConfig.REDIS_PORT,
        db=AlertConfig.REDIS_DB,
        ttl_seconds=AlertConfig.ALERT_DEDUP_TTL_SECONDS,
        socket_timeout_seconds=AlertConfig.REDIS_SOCKET_TIMEOUT_SECONDS,
        socket_connect_timeout_seconds=AlertConfig.REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS,
    )
    notifier = TelegramNotifier(
        token=AlertConfig.TELEGRAM_TOKEN,
        chat_id=AlertConfig.TELEGRAM_CHAT_ID,
    )


    stats = {
        "consumed": 0,
        "sent": 0,
        "skipped_severity": 0,
        "duplicates": 0,
        "invalid": 0,
        "consumer_errors": 0,
        "send_failures": 0,
    }
    last_stats_at = time.time()
    last_message_at = None

    try:
        print("[info] Alert service is listening...")
        while True:
            msg = consumer.poll(AlertConfig.KAFKA_POLL_TIMEOUT)
            if msg is None:
                now = time.time()
                if now - last_stats_at >= AlertConfig.ALERT_STATS_INTERVAL_SECONDS:
                    _log_stats(stats, last_message_at)
                    last_stats_at = now
                continue

            if msg.error():
                stats["consumer_errors"] += 1
                print(f"[error] Consumer error: {msg.error()}")
                continue

            stats["consumed"] += 1
            last_message_at = time.time()

            alert = _decode_message(msg)
            if alert is None:
                stats["invalid"] += 1
                _commit_message(consumer, msg)
                continue

            severity = (alert.get("severity") or "").lower()
            if not should_notify(severity):
                stats["skipped_severity"] += 1
                _commit_message(consumer, msg)
                continue

            dedupe_key = _message_key(msg) or _fallback_key(alert)
            if not deduplicator.claim(dedupe_key):
                stats["duplicates"] += 1
                print(f"[info] Duplicate alert skipped: {dedupe_key}")
                _commit_message(consumer, msg)
                continue

            try:
                notifier.send_alert(alert)
                stats["sent"] += 1
                print(f"[info] Alert sent: {dedupe_key}")
                _commit_message(consumer, msg)
            except Exception as exc:
                stats["send_failures"] += 1
                # Release key for retries if send failed.
                deduplicator.release(dedupe_key)
                print(f"[error] Failed to send alert: {exc}")

            now = time.time()
            if now - last_stats_at >= AlertConfig.ALERT_STATS_INTERVAL_SECONDS:
                _log_stats(stats, last_message_at)
                last_stats_at = now

    except KeyboardInterrupt:
        print("[info] Shutting down alert service...")
    finally:
        notifier.close()
        consumer.close()


if __name__ == "__main__":
    main()
