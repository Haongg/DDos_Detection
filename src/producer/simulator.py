import json
import os
import random
import time
import math
import concurrent.futures
from urllib.parse import urlencode

import numpy as np
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker

def kafka_topics(producer_conf):
    admin_client = AdminClient({'bootstrap.servers': producer_conf['bootstrap.servers']})
    target_topic = os.getenv('KAFKA_TOPIC', 'network-traffic')

    metadata = admin_client.list_topics(timeout=5)
    if target_topic not in metadata.topics:
        print(f"Topic '{target_topic}' not exist. Creating...")

        new_topic = NewTopic(target_topic, num_partitions=3, replication_factor=1)
        fs = admin_client.create_topics([new_topic])

        for topic, f in fs.items():
            try:
                f.result()
                print(f"✅ Successful: {topic}")
            except Exception as e:
                print(f"❌ Errors: {e}")
    else:
        print(f"Topic '{target_topic}' already created.")

class TrafficResources:
    def __init__(self):
        self.fake = Faker()
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) Safari/604.1",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Firefox/115.0"
        ]
        self.normal_paths = ["/", "/home", "/products", "/about", "/contact", "/cart", "/blog"]
        self.search_terms = ["wireless keyboard", "gaming laptop", "office chair", "usb-c hub", "4k monitor"]
        self.utm_sources = ["google", "facebook", "newsletter", "affiliate", "direct"]

        self.flood_ip = [self.fake.ipv4() for _ in range(100)]
        self.botnet_ips = [self.fake.ipv4() for _ in range(500)]
        self.heavy_endpoints = ["/api/v1/search", "/api/v1/report/export", "/api/v1/login"]
        self.scan_endpoints = ["/.env", "/wp-admin", "/admin/config.php", "/etc/passwd"]

        self.method = ["GET", "POST", "PUT", "DELETE"]

        self.status_code = [200, 201, 400, 401, 403, 404, 500, 502, 503]

class RPSController:
    def __init__(self, base_rps=100):
        self.base_rps = base_rps
        self.start_time = time.time()

    def get_current_rps(self):
        elapsed = time.time() - self.start_time
        seasonal_factor = 1 + 0.5 * math.sin(2 * math.pi * elapsed / 60)
        noise = random.uniform(0.9, 1.1)
        return int(self.base_rps * seasonal_factor * noise)

    def get_inter_arrival_time(self, rps):
        return np.random.exponential(1.0 / rps)

class TrafficFactory(TrafficResources):
    def __init__(self, producer, topic):
        super().__init__()
        self.producer = producer
        self.topic = topic

    def send(self, data):
        self.producer.produce(self.topic, key=data['src_ip'], value=json.dumps(data))

    def _build_url(self, path, params=None):
        if not params:
            return path
        query = urlencode(params, doseq=True)
        return f"{path}?{query}"

    def _random_normal_url(self, method):
        if method in ("POST", "PUT", "DELETE"):
            api_paths = {
                "POST": "/api/v1/cart/items",
                "PUT": f"/api/v1/users/{random.randint(1000, 9999)}/profile",
                "DELETE": f"/api/v1/cart/items/{random.randint(1, 10)}"
            }
            return self._build_url(
                api_paths[method],
                {"session_id": self.fake.uuid4()[:12], "client": random.choice(["web", "mobile"])}
            )

        roll = random.random()
        if roll < 0.3:
            return self._build_url(
                f"/products/{random.randint(1000, 9999)}",
                {"ref": random.choice(["home", "recommendation", "flash_sale"])}
            )
        if roll < 0.6:
            return self._build_url(
                "/search",
                {
                    "q": random.choice(self.search_terms),
                    "sort": random.choice(["relevance", "price_asc", "newest"]),
                    "page": random.randint(1, 5)
                }
            )
        if roll < 0.8:
            return self._build_url(
                f"/blog/{self.fake.slug()}",
                {"utm_source": random.choice(self.utm_sources), "utm_medium": "cpc"}
            )
        return self._build_url(
            random.choice(self.normal_paths),
            {"session_id": self.fake.uuid4()[:12]}
        )

    def _random_heavy_url(self):
        base = random.choice(self.heavy_endpoints)
        if base == "/api/v1/search":
            return self._build_url(
                base,
                {"q": self.fake.sentence(nb_words=4), "limit": random.choice([50, 100, 200]), "full_text": "true"}
            )
        if base == "/api/v1/report/export":
            return self._build_url(
                base,
                {"from": self.fake.date_this_year(), "to": self.fake.date_this_year(), "format": random.choice(["csv", "xlsx"])}
            )
        return self._build_url(base, {"redirect": "/dashboard", "locale": random.choice(["en", "vi", "ja"])})

    def _base_packet(self, ip, ua, url,stt_code=200, method="GET"):
        return {
            "timestamp": int(time.time() * 1000),
            "src_ip": ip,
            "dst_ip": "10.0.0.1",
            "protocol": "HTTP/1.1",
            "method": method,
            "user_agent": ua,
            "request_url": url,
            "status_code": stt_code,
            "response_size": random.randint(100, 1024)
        }

    # --- VOLUME ---
    def method_flood(self):
        """DDos Flood"""
        ip = random.choice(self.flood_ip)
        for _ in range(100):
            packet = self._base_packet(
                ip,
                random.choice(self.user_agents),
                self._build_url("/api/v1/login", {"username": self.fake.user_name(), "nonce": self.fake.uuid4()[:8]}),
                random.choice([200, 201]),
                random.choice(self.method)
            )
            self.send(packet)

    def method_botnet(self):
        """Botnet"""
        for ip in random.sample(self.botnet_ips, 50):
            method = random.choice(self.method)
            packet = self._base_packet(
                ip,
                random.choice(self.user_agents),
                self._random_normal_url(method),
                random.choice([200, 201]),
                method
            )
            self.send(packet)

    # --- BEHAVIOR ---
    def method_search_flood(self):
        """DDos Search Flood"""
        packet = self._base_packet(
            self.fake.ipv4(),
            random.choice(self.user_agents),
            self._random_heavy_url(),
            random.choice(self.status_code),
            random.choice(self.method)
        )
        packet['payload_size'] = random.randint(1024, 2048)
        self.send(packet)

    def method_scanning(self):
        """DDos Scanning"""
        packet = self._base_packet(self.fake.ipv4(), "Nmap-Scanner/7.9", random.choice(self.scan_endpoints), 404, "GET")
        self.send(packet)

    # --- VULNERABILITY ---
    def method_slowloris(self):
        """Slowloris Attack"""
        packet = self._base_packet(self.fake.ipv4(), "Slowloris-Lib/1.1", "/index.html", 408, "GET")
        packet['payload_size'] = random.randint(1, 10)
        self.send(packet)

    # --- NORMAL GENERATOR ---
    def gen_normal(self, count):
        for _ in range(count):
            method = random.choices(self.method, weights=[0.8, 0.12, 0.05, 0.03], k=1)[0]
            packet = self._base_packet(
                self.fake.ipv4(),
                random.choice(self.user_agents),
                self._random_normal_url(method),
                random.choices(self.status_code, weights=[0.76, 0.08, 0.03, 0.03, 0.03, 0.04, 0.01, 0.01, 0.01], k=1)[0],
                method
            )
            self.send(packet)

class TrafficOrchestrator:
    def __init__(self, factory, controller):
        self.factory = factory
        self.controller = controller
        self.stop_signal = False

    def run_normal_stream(self):
        """Luồng 1: Giả lập người dùng thật chạy liên tục với Dynamic RPS"""
        print("🟢 [Thread-Normal] Started.")
        while not self.stop_signal:
            current_rps = self.controller.get_current_rps()
            batch_size = max(1, int(current_rps * 0.1))
            self.factory.gen_normal(batch_size)
            
            self.factory.producer.poll(0)
            
            time.sleep(self.controller.get_inter_arrival_time(current_rps))

    def run_attack_stream(self):
        """Luồng 2: Giả lập các đợt tấn công bùng nổ ngẫu nhiên"""
        print("🔴 [Thread-Attack] Started.")
        while not self.stop_signal:
            time.sleep(random.randint(10, 20))
            
            if self.stop_signal: break

            choice = random.random()
            if choice < 0.2: 
                print("🔥 [ATTACK] Launching HTTP Flood...")
                self.factory.method_flood()
            elif choice < 0.4: 
                print("🤖 [ATTACK] Launching Botnet...")
                self.factory.method_botnet()
            elif choice < 0.6: 
                print("🔎 [ATTACK] Vulnerability Scanning...")
                self.factory.method_scanning()
            elif choice < 0.8: 
                print("🔎 [ATTACK] Search Flood...")
                self.factory.method_search_flood()
            else: 
                print("⏳ [ATTACK] Slowloris Attempt...")
                self.factory.method_slowloris()
            
            self.factory.producer.flush()

def main():
    kafka_server = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092').strip()
    # print(f"DEBUG: Connecting to Kafka at -> {kafka_server}")

    conf = {
        'bootstrap.servers': kafka_server,
        'client.id': 'ddos-simulator',
        'acks': 1,
        'message.timeout.ms': 30000,
        'request.timeout.ms': 20000,
        'queue.buffering.max.kbytes': 33554432,
        'batch.num.messages': 131072,
        'linger.ms': 100
    }

    kafka_topics(conf)

    p = Producer(conf)
    factory = TrafficFactory(p, os.getenv('KAFKA_TOPIC', 'network-traffic'))
    controller = RPSController(base_rps=50)
    orchestrator = TrafficOrchestrator(factory, controller)

    print("🚀 Orchestrator started. Multi-threading active...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(orchestrator.run_normal_stream),
            executor.submit(orchestrator.run_attack_stream)
        ]
        
        try:
            concurrent.futures.wait(futures)
        except KeyboardInterrupt:
            print("\n🛑 Shutting down. Flushing Kafka...")
            orchestrator.stop_signal = True
            p.flush()

if __name__ == "__main__":
    main()
