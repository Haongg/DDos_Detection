from confluent_kafka import Consumer
import json
import requests
import os

conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'group.id': "alert-service-group",
    'auto.offset.reset': 'lastest',
    'enable.auto.commit': False
}

class TelegramNotifier:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.api_url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    def send_alert(self, message):
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        try:
            response = requests.post(self.api_url, json=payload, timeout=10)
            response.raise_for_status()
            print("✅ Alert sent to Telegram successfully!")
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to send alert to Telegram: {e}")  

def main():
  consumer = Consumer(conf)
  consumer.subscribe(['ddos-alerts'])
  notifier = TelegramNotifier()
  print("🚀 Alert Service is listening for DDoS attacks...")
  try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None: continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            alert_data = json.loads(msg.value().decode('utf-8'))
        except json.JSONDecodeError as e:
            print(f"❌ Failed to decode message: {e}")
            continue

        ip = alert_data.get('src_ip')
        attack = alert_data.get('attack_type')
        severity = alert_data.get('severity')

        print(f"🚨 Sending Alert for {ip} - {attack} - {severity}")

        try:
            notifier.send_alert(f"🚨 DDoS Attack Detected!\nIP: {ip}\nAttack: {attack}\nSeverity: {severity}")
            consumer.commit()
        except Exception as e:
            print(f"❌ Error sending alert: {e}")

  except KeyboardInterrupt:
      print("Shutting down Alert Service...")
  finally:
      print("🧹 Closing Kafka Consumer safely...")
      consumer.close()

if __name__ == "__main__":
  main()