"""LangSmith API client for fetching runs."""
from __future__ import annotations

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

# Constants
MAX_RETRIES = 3
REQUEST_TIMEOUT = 60
ESTIMATION_MIN_PAGES = 2
RECENT_PAGES_SAMPLE = 3


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
    
    def _make_api_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Make API request with retry logic for rate limits and errors."""
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.post(
                    LANGSMITH_QUERY_URL, 
                    headers=self.headers, 
                    json=payload, 
                    timeout=REQUEST_TIMEOUT
                )
                
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 429:  # Rate limit
                    logging.warning("Rate limit hit (attempt %d/%d), retrying...", attempt + 1, MAX_RETRIES)
                    if attempt < MAX_RETRIES - 1:
                        continue
                else:
                    # Other HTTP errors
                    logging.warning(
                        "HTTP %d error (attempt %d/%d): %s", 
                        resp.status_code, attempt + 1, MAX_RETRIES, resp.text[:200]
                    )
                    if attempt < MAX_RETRIES - 1:
                        continue
            
            except requests.exceptions.RequestException as e:
                logging.warning("Request error (attempt %d/%d): %s", attempt + 1, MAX_RETRIES, str(e))
                if attempt < MAX_RETRIES - 1:
                    continue
            
            # If we get here on the last attempt, raise the error
            if attempt == MAX_RETRIES - 1:
                if 'resp' in locals() and resp.status_code != 200:
                    raise RuntimeError(
                        f"LangSmith query failed after {MAX_RETRIES} attempts (HTTP {resp.status_code}): {resp.text[:400]}"
                    )
                else:
                    raise RuntimeError(f"LangSmith query failed after {MAX_RETRIES} attempts due to request errors")
    
    def _create_query_payload(self, start_time: datetime, end_time: datetime, cursor: str = "") -> Dict[str, Any]:
        """Create the API query payload."""
        return {
            "cursor": cursor,
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
    
    def _should_stop_debug_mode(self, all_runs: List[Dict[str, Any]], debug_limit: Optional[int]) -> bool:
        """Check if we should stop due to debug limit."""
        return debug_limit is not None and len(all_runs) >= debug_limit
    
    def _log_page_progress(self, page_index: int, runs_fetched: int, total_runs: int, estimation_info: str) -> None:
        """Log progress information for the current page."""
        logging.info(
            "Page %s: fetched %s runs; total so far: %s; %s",
            page_index,
            runs_fetched,
            total_runs,
            estimation_info,
        )
    
    def fetch_all_runs(self, start_time: datetime, end_time: datetime, debug_limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch all runs from LangSmith API with pagination."""
        all_runs: List[Dict[str, Any]] = []
        page_index = 0
        runs_per_page_history: List[int] = []
        cursor = ""

        while True:
            page_index += 1
            
            # Create payload and make API request
            payload = self._create_query_payload(start_time, end_time, cursor)
            data = self._make_api_request(payload)
            
            # Process response
            runs = data.get("runs", [])
            if not isinstance(runs, list):
                raise RuntimeError("Unexpected response format: 'runs' is not a list")

            # Update collections
            all_runs.extend(runs)
            runs_per_page_history.append(len(runs))

            # Get next cursor
            next_cursor = (data.get("cursors") or {}).get("next")

            # Generate estimation and log progress
            estimation_info = self._estimate_progress(
                runs_per_page_history, len(all_runs), bool(next_cursor), start_time, end_time, all_runs
            )
            self._log_page_progress(page_index, len(runs), len(all_runs), estimation_info)

            # Check debug mode limit
            if self._should_stop_debug_mode(all_runs, debug_limit):
                logging.info("🐛 Debug mode enabled: stopping after %d runs.", debug_limit)
                all_runs = all_runs[:debug_limit]  # Trim to exact limit
                break

            # Continue pagination or finish
            if next_cursor:
                cursor = next_cursor
                continue
            else:
                break

        return all_runs
    
    def _estimate_progress(
        self, 
        runs_per_page_history: List[int], 
        total_runs_so_far: int, 
        has_next_cursor: bool,
        start_time: datetime,
        end_time: datetime,
        all_runs: List[Dict[str, Any]]
    ) -> str:
        """Estimate progress and remaining runs based on available data."""
        if not has_next_cursor:
            return "100% completed"
        
        if len(runs_per_page_history) < ESTIMATION_MIN_PAGES:
            return "estimating..."
        
        estimated_remaining_runs = self._calculate_remaining_runs_estimate(runs_per_page_history)
        estimated_total = total_runs_so_far + estimated_remaining_runs
        
        # Calculate progress percentage
        progress_percent = int((total_runs_so_far / estimated_total) * 100) if estimated_total > 0 else 0
        progress_percent = min(progress_percent, 95)  # Cap at 95% since we're estimating
        
        return f"{progress_percent}% complete"
    
    def _calculate_remaining_runs_estimate(self, runs_per_page_history: List[int]) -> int:
        """Calculate estimated remaining runs based on page history patterns."""
        avg_runs_per_page = sum(runs_per_page_history) / len(runs_per_page_history)
        recent_pages = runs_per_page_history[-RECENT_PAGES_SAMPLE:]
        recent_avg = sum(recent_pages) / len(recent_pages)
        
        # If recent pages are getting smaller, we might be near the end
        if self._is_trending_downward(recent_avg, avg_runs_per_page, len(runs_per_page_history)):
            estimated_remaining_pages = 1 + len([x for x in recent_pages if x > 0])
            return int(recent_avg * estimated_remaining_pages)
        else:
            # Use conservative estimate based on average
            return int(avg_runs_per_page * 2)
    
    def _is_trending_downward(self, recent_avg: float, overall_avg: float, total_pages: int) -> bool:
        """Check if recent pages show a downward trend indicating we're near the end."""
        return recent_avg < overall_avg * 0.5 and total_pages > 3
