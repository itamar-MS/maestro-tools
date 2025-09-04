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
from data_processor import _parse_iso
from thread_parser import enrich_run_with_thread_data

LANGSMITH_QUERY_URL = "https://api.smith.langchain.com/api/v1/runs/query"

# Constants
MAX_RETRIES = 3
REQUEST_TIMEOUT = 60


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
    
    def _clean_empty_fields(self, run: Dict[str, Any]) -> Dict[str, Any]:
        """Remove empty fields from a run to reduce JSON size."""
        if not isinstance(run, dict):
            return run
        
        cleaned = {}
        for key, value in run.items():
            # Skip empty values (None, empty strings, empty lists, empty dicts)
            if value is None:
                continue
            elif isinstance(value, str) and value.strip() == "":
                continue
            elif isinstance(value, (list, dict)) and len(value) == 0:
                continue
            else:
                # Recursively clean nested dictionaries
                if isinstance(value, dict):
                    cleaned_value = self._clean_empty_fields(value)
                    if cleaned_value:  # Only add if not empty after cleaning
                        cleaned[key] = cleaned_value
                else:
                    cleaned[key] = value
        
        return cleaned
    
    def _deduplicate_incrementally(self, new_runs: List[Dict[str, Any]], existing_runs_by_thread: Dict[str, Dict[str, Any]]) -> int:
        """
        Deduplicate new runs against existing ones, keeping only the latest per thread_id.
        Updates the existing_runs_by_thread dict in place.
        
        Returns:
            Number of runs that were excluded as duplicates
        """
        duplicates_excluded = 0
        
        for run in new_runs:
            thread_id = (run or {}).get("thread_id")
            if not thread_id:
                # Keep items without thread_id uniquely by id to avoid accidental drops
                thread_id = f"_no_thread_{(run or {}).get('id', len(existing_runs_by_thread))}"
            
            ts = _parse_iso((run or {}).get("start_time"))
            
            existing = existing_runs_by_thread.get(thread_id)
            if not existing:
                # Enrich, clean, and store new run
                enriched_run = enrich_run_with_thread_data(run)
                cleaned_run = self._clean_empty_fields(enriched_run)
                existing_runs_by_thread[thread_id] = cleaned_run
            else:
                existing_ts = _parse_iso((existing or {}).get("start_time"))
                if ts > existing_ts:
                    # Replace with newer run
                    enriched_run = enrich_run_with_thread_data(run)
                    cleaned_run = self._clean_empty_fields(enriched_run)
                    existing_runs_by_thread[thread_id] = cleaned_run
                    duplicates_excluded += 1
                else:
                    # Keep existing, exclude this one
                    duplicates_excluded += 1
        
        return duplicates_excluded
    
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
        """Fetch all runs from LangSmith API with pagination and incremental deduplication."""
        runs_by_thread: Dict[str, Dict[str, Any]] = {}
        page_index = 0
        cursor = ""
        total_duplicates_excluded = 0
        total_runs_fetched = 0

        while True:
            page_index += 1
            
            # Create payload and make API request
            payload = self._create_query_payload(start_time, end_time, cursor)
            data = self._make_api_request(payload)
            
            # Process response
            runs = data.get("runs", [])
            if not isinstance(runs, list):
                raise RuntimeError("Unexpected response format: 'runs' is not a list")

            total_runs_fetched += len(runs)
            
            # Deduplicate incrementally
            duplicates_excluded = self._deduplicate_incrementally(runs, runs_by_thread)
            total_duplicates_excluded += duplicates_excluded

            # Get next cursor
            next_cursor = (data.get("cursors") or {}).get("next")

            # Log progress
            has_more = "has more pages" if next_cursor else "completed"
            self._log_page_progress(page_index, len(runs), len(runs_by_thread), has_more)

            # Check debug mode limit (based on unique runs after deduplication)
            if self._should_stop_debug_mode(list(runs_by_thread.values()), debug_limit):
                logging.info("🐛 Debug mode enabled: stopping after %d unique runs.", debug_limit)
                # Trim to exact limit
                unique_runs = list(runs_by_thread.values())[:debug_limit]
                runs_by_thread = {run.get('thread_id', f'_no_thread_{i}'): run for i, run in enumerate(unique_runs)}
                break

            # Continue pagination or finish
            if next_cursor:
                cursor = next_cursor
                continue
            else:
                break

        # Log final deduplication stats
        final_runs = list(runs_by_thread.values())
        logging.info("Total runs fetched from API: %s", total_runs_fetched)
        logging.info("Total runs after deduplication: %s", len(final_runs))
        logging.info("Total excluded as older duplicates: %s", total_duplicates_excluded)
        logging.info("Enriched runs with user_id and lesson_id from thread_id")
        logging.info("Cleaned empty fields from all runs")

        return final_runs
