import json
import time
import urllib.error
import urllib.request

from schema import (
  es_base_url,
  ILM_POLICY,
  ILM_POLICY_NAME,
  INDEX_TEMPLATE,
  INDEX_TEMPLATE_NAME,
  WRITE_ALIAS,
)


def _request(method, path, payload=None, allow_statuses=None):
  url = f"{es_base_url()}{path}"
  data = None
  headers = {"Content-Type": "application/json"}
  if payload is not None:
    data = json.dumps(payload).encode("utf-8")
  req = urllib.request.Request(url, data=data, headers=headers, method=method)
  try:
    with urllib.request.urlopen(req) as resp:
      body = resp.read().decode("utf-8")
      return resp.status, body
  except urllib.error.HTTPError as exc:
    if allow_statuses and exc.code in allow_statuses:
      body = exc.read().decode("utf-8")
      return exc.code, body
    raise


def wait_for_es(retries=30, delay_seconds=2):
  for _ in range(retries):
    try:
      status, _ = _request("GET", "/_cluster/health")
      if status == 200:
        return True
    except Exception:
      time.sleep(delay_seconds)
  return False


def main():
  if not wait_for_es():
    raise SystemExit("Elasticsearch not ready")

  status, body = _request("PUT", f"/_ilm/policy/{ILM_POLICY_NAME}", ILM_POLICY, allow_statuses=[200])
  print(f"[es-init] ILM policy status={status} body={body}")

  status, body = _request("PUT", f"/_index_template/{INDEX_TEMPLATE_NAME}", INDEX_TEMPLATE, allow_statuses=[200])
  print(f"[es-init] index template status={status} body={body}")

  # Create initial rollover index if missing
  status, body = _request(
    "PUT",
    f"/{WRITE_ALIAS}-000001",
    {"aliases": {WRITE_ALIAS: {"is_write_index": True}}},
    allow_statuses=[200, 400, 409],
  )
  print(f"[es-init] initial index status={status} body={body}")


if __name__ == "__main__":
  main()
