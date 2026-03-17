import os

class ESConfig:
    def __init__(self):
        self.scheme = os.getenv("ES_SCHEME", "http")
        self.host = os.getenv("ES_HOST", "elasticsearch")
        self.port = int(os.getenv("ES_PORT", "9200"))

    @property
    def base_url(self):
        return f"{self.scheme}://{self.host}:{self.port}"
