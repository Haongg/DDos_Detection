import time


class AlertBatcher:
    def __init__(self, window_seconds=10, max_ips=50):
        self.window_seconds = max(0, int(window_seconds))
        self.max_ips = max(1, int(max_ips))
        self._batches = {}

    @staticmethod
    def _key(alert):
        return (
            str(alert.get("attack_type", "unknown")),
            str(alert.get("severity", "unknown")),
            str(alert.get("window_size", "unknown")),
            str(alert.get("signal_reason", "unknown")),
        )

    def add(self, alert, now=None):
        if self.window_seconds <= 0:
            return []

        if now is None:
            now = time.monotonic()

        key = self._key(alert)
        batch = self._batches.get(key)
        if batch is None:
            batch = {
                "created_at": now,
                "attack_type": alert.get("attack_type"),
                "severity": alert.get("severity"),
                "window_size": alert.get("window_size"),
                "signal_reason": alert.get("signal_reason"),
                "window_end_min": alert.get("window_end"),
                "window_end_max": alert.get("window_end"),
                "ips": [],
                "ip_set": set(),
                "request_count_sum": 0.0,
                "rps_sum": 0.0,
                "alerts": 0,
            }
            self._batches[key] = batch

        src_ip = alert.get("src_ip", "unknown")
        if src_ip not in batch["ip_set"]:
            if len(batch["ips"]) < self.max_ips:
                batch["ips"].append(src_ip)
            batch["ip_set"].add(src_ip)

        batch["alerts"] += 1

        try:
            batch["request_count_sum"] += float(alert.get("request_count", 0) or 0)
        except (TypeError, ValueError):
            pass

        try:
            batch["rps_sum"] += float(alert.get("rps", 0) or 0)
        except (TypeError, ValueError):
            pass

        window_end = alert.get("window_end")
        if window_end is not None:
            if batch["window_end_min"] is None or window_end < batch["window_end_min"]:
                batch["window_end_min"] = window_end
            if batch["window_end_max"] is None or window_end > batch["window_end_max"]:
                batch["window_end_max"] = window_end

        return self.flush_if_due(now)

    def flush_if_due(self, now=None):
        if self.window_seconds <= 0:
            return []

        if now is None:
            now = time.monotonic()

        due = []
        for key, batch in list(self._batches.items()):
            if now - batch["created_at"] >= self.window_seconds:
                due.append(batch)
                self._batches.pop(key, None)
        return due

    def flush_all(self):
        batches = list(self._batches.values())
        self._batches.clear()
        return batches
