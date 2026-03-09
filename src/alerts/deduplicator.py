import redis
from redis.exceptions import RedisError


class AlertDeduplicator:
    def __init__(
        self,
        host,
        port,
        db,
        ttl_seconds,
        socket_timeout_seconds=2.0,
        socket_connect_timeout_seconds=2.0,
    ):
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True,
            socket_timeout=socket_timeout_seconds,
            socket_connect_timeout=socket_connect_timeout_seconds,
            health_check_interval=30,
        )
        self.ttl_seconds = ttl_seconds

    @staticmethod
    def _redis_key(dedupe_key):
        return f"ddos:alert:{dedupe_key}"

    def claim(self, dedupe_key):
        key = self._redis_key(dedupe_key)
        try:
            created = self.client.set(key, "1", nx=True, ex=self.ttl_seconds)
            return bool(created)
        except RedisError as exc:
            # Fail-open: prefer duplicate alerts over missing critical alerts.
            print(f"[warn] Redis unavailable, bypass dedupe: {exc}")
            return True

    def release(self, dedupe_key):
        key = self._redis_key(dedupe_key)
        try:
            self.client.delete(key)
        except RedisError as exc:
            print(f"[warn] Failed to release dedupe key {key}: {exc}")
