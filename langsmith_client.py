"""LangSmith API client for fetching runs."""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
import logging

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

from config import Config

LANGSMITH_QUERY_URL = "https://api.smith.langchain.com/api/v1/runs/query"


def _to_iso(dt: datetime) -> str:
    """Convert datetime to ISO 8601 format with UTC timezone."""
    if dt.tzinfo is None:
        return dt.replace(microsecond=0).isoformat() + "Z"
    return dt.astimezone(ZoneInfo("UTC")).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class LangSmithClient:
    """Client for interacting with LangSmith API."""
    
    def __init__(self, config: Config):
        self.config = config
        self.headers = {
            "x-api-key": config.langsmith_api_key,
            "Content-Type": "application/json",
        }
    
    def fetch_all_runs(self, start_time: datetime, end_time: datetime, debug_limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch all runs from LangSmith API with pagination."""
        payload: Dict[str, Any] = {
            "cursor": "",
            "limit": self.config.limit,
            "session": self.config.session_ids,
            "is_root": True,
            "start_time": _to_iso(start_time),
            "end_time": _to_iso(end_time),
            "order_by": "start_time",
            "select": [
                "id",
                "trace_id",
                "thread_id",
                "name",
                "outputs",
                "start_time",
            ],
            "filter": f'eq(name, "{self.config.filter_name}")',
        }

        all_runs: List[Dict[str, Any]] = []
        page_index = 0

        while True:
            page_index += 1
            resp = requests.post(LANGSMITH_QUERY_URL, headers=self.headers, json=payload, timeout=60)
            
            if resp.status_code != 200:
                raise RuntimeError(
                    f"LangSmith query failed (HTTP {resp.status_code}): {resp.text[:400]}"
                )
            
            data = resp.json()
            runs = data.get("runs", [])
            
            if not isinstance(runs, list):
                raise RuntimeError("Unexpected response format: 'runs' is not a list")

            all_runs.extend(runs)

            next_cursor = (
                (data.get("cursors") or {}).get("next")
                if isinstance(data, dict)
                else None
            )

            logging.info(
                "Page %s: fetched %s runs; total so far: %s; next_cursor=%s",
                page_index,
                len(runs),
                len(all_runs),
                bool(next_cursor),
            )

            # Debug mode: limit total runs fetched
            if debug_limit and len(all_runs) >= debug_limit:
                logging.info("🐛 Debug mode enabled: stopping after %d runs.", debug_limit)
                # Trim to exact limit
                all_runs = all_runs[:debug_limit]
                break

            if next_cursor:
                payload["cursor"] = next_cursor
                # Wait 1 second between paginated calls (mimic n8n behavior)
                time.sleep(1)
                continue
            
            break

        return all_runs
