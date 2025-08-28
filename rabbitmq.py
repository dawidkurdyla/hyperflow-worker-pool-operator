# Minimal RabbitMQ Management API client for queue lifecycle.

import time
import requests
from urllib.parse import quote

class RabbitAdmin:
    def __init__(self, api_base: str, user: str, password: str, timeout: int = 5, verify: bool = True):
        self.api = api_base.rstrip('/')
        self.auth = (user, password)
        self.timeout = timeout
        self.verify = verify

    def _q(self, vhost: str, name: str) -> str:
        return f"{self.api}/queues/{quote(vhost, safe='')}/{quote(name, safe='')}"

    def ensure_queue(self, vhost: str, name: str, durable=True, auto_delete=False, arguments=None, attempts: int = 5):
        url = self._q(vhost, name)
        payload = {
            "durable": bool(durable),
            "auto_delete": bool(auto_delete),
            "arguments": arguments or {},
        }

        last = None
        for i in range(attempts):
            last = requests.put(url, json=payload, auth=self.auth, timeout=self.timeout, verify=self.verify)
            if last.status_code in (200, 201, 204):
                return True
            time.sleep(min(2 ** i, 10))
        raise RuntimeError(f"Queue PUT failed: {last.status_code} {last.text if last else ''}")

    def delete_queue(self, vhost: str, name: str):
        url = self._q(vhost, name)
        r = requests.delete(url, auth=self.auth, timeout=self.timeout, verify=self.verify)
        if r.status_code in (204, 404):
            return True
        raise RuntimeError(f"Queue DELETE failed: {r.status_code} {r.text}")

    def get_queue(self, vhost: str, name: str):
        url = self._q(vhost, name)
        r = requests.get(url, auth=self.auth, timeout=self.timeout, verify=self.verify)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 404:
            return None
        raise RuntimeError(f"Queue GET failed: {r.status_code} {r.text}")
