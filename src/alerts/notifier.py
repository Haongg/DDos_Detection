import requests


class TelegramNotifier:
    def __init__(self, token, chat_id, timeout_seconds=8):
        if not token or not chat_id:
            raise ValueError("Missing TELEGRAM_TOKEN or TELEGRAM_CHAT_ID")

        self.api_url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.chat_id = chat_id
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    @staticmethod
    def _format_alert(alert):
        return (
            "DDoS Alert\n"
            f"IP: {alert.get('src_ip', 'unknown')}\n"
            f"Attack: {alert.get('attack_type', 'unknown')}\n"
            f"Severity: {alert.get('severity', 'unknown')}\n"
            f"Window: {alert.get('window_size', 'unknown')} ending {alert.get('window_end', 'unknown')}\n"
            f"Requests: {alert.get('request_count', 'unknown')} | RPS: {alert.get('rps', 'unknown')}\n"
            f"Reason: {alert.get('signal_reason', 'unknown')}"
        )

    def send_alert(self, alert):
        payload = {
            "chat_id": self.chat_id,
            "text": self._format_alert(alert),
        }
        response = self.session.post(
            self.api_url,
            json=payload,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        body = response.json()
        if not body.get("ok", False):
            raise RuntimeError(f"Telegram API error: {body}")

    def close(self):
        self.session.close()
