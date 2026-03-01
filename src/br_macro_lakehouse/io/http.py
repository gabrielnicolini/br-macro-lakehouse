from __future__ import annotations

import time
from typing import Any

import requests


def get_json(url: str, params: dict[str, Any] | None = None, timeout_s: int = 30, retries: int = 3) -> Any:
    last_err: Exception | None = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout_s)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"GET failed: {url}. Last error: {last_err}")
