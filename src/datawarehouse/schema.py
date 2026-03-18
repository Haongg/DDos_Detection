import os

ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_PORT = int(os.getenv("ES_PORT", "9200"))
ES_SCHEME = os.getenv("ES_SCHEME", "http")

ILM_POLICY_NAME = "ddos-alerts-delete-after-1d"
INDEX_TEMPLATE_NAME = "ddos-alerts-template"
INDEX_PATTERN = "ddos-alerts-*"

ILM_POLICY = {
  "policy": {
    "phases": {
      "delete": {
        "min_age": "1d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}

INDEX_TEMPLATE = {
  "index_patterns": [INDEX_PATTERN],
  "template": {
    "settings": {
      "index.lifecycle.name": ILM_POLICY_NAME,
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "refresh_interval": "5s"
    },
    "mappings": {
      "dynamic": True,
      "properties": {
        "@timestamp": {"type": "date"},
        "src_ip": {"type": "keyword"},
        "attack_type": {"type": "keyword"},
        "severity": {"type": "keyword"},
        "severity_rank": {"type": "integer"},
        "window_start": {"type": "date"},
      "window_end": {"type": "date"},
      "window_size": {"type": "keyword"},
      "request_count": {"type": "long"},
      "rps": {"type": "double"},
      "unique_path_count": {"type": "integer"},
      "error_ratio": {"type": "double"},
      "signal_reason": {"type": "keyword"},
      "detected_at": {"type": "date"}
      }
    }
  },
  "priority": 200
}


def es_base_url():
  return f"{ES_SCHEME}://{ES_HOST}:{ES_PORT}"
