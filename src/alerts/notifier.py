import requests
import os

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

    @staticmethod
    def _format_batch(batch):
        ip_list = batch.get("ips") or []
        ip_count = len(batch.get("ip_set") or ip_list)
        ip_text = ", ".join(ip_list)
        if ip_count > len(ip_list):
            ip_text = f"{ip_text}, ...(+{ip_count - len(ip_list)} more)"

        window_end = batch.get("window_end_max") or batch.get("window_end_min") or "unknown"
        request_count = batch.get("request_count_sum")
        rps = batch.get("rps_sum")
        return (
            "DDoS Alert\n"
            f"IP: {ip_text or 'unknown'}\n"
            f"Attack: {batch.get('attack_type', 'unknown')}\n"
            f"Severity: {batch.get('severity', 'unknown')}\n"
            f"Window: {batch.get('window_size', 'unknown')} ending {window_end}\n"
            f"Requests: {request_count:.0f} | RPS: {rps:.2f}\n"
            f"Reason: {batch.get('signal_reason', 'unknown')}"
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

    def send_batch(self, batch):
        payload = {
            "chat_id": self.chat_id,
            "text": self._format_batch(batch),
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
