"""Data processing utilities for LangSmith runs."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from thread_parser import enrich_run_with_thread_data


def _parse_iso(dt_str: Optional[str]) -> float:
    """Parse ISO 8601 string to POSIX timestamp seconds. Unknown -> 0.0"""
    if not dt_str:
        return 0.0
    try:
        # Accept trailing Z
        if dt_str.endswith("Z"):
            dt_obj = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        else:
            dt_obj = datetime.fromisoformat(dt_str)
        return dt_obj.timestamp()
    except Exception:
        return 0.0


def deduplicate_by_thread_latest(runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplicate runs by thread_id, keeping only the latest run based on start_time.
    Also enriches each run with user_id and lesson_id parsed from thread_id.
    
    Args:
        runs: List of run dictionaries from LangSmith API
        
    Returns:
        List of deduplicated and enriched runs
    """
    latest_by_thread: Dict[str, Dict[str, Any]] = {}
    duplicates_excluded = 0

    for run in runs:
        thread_id = (run or {}).get("thread_id")
        if not thread_id:
            # Keep items without thread_id uniquely by id to avoid accidental drops
            thread_id = f"_no_thread_{(run or {}).get('id', len(latest_by_thread))}"
        
        ts = _parse_iso((run or {}).get("start_time"))

        existing = latest_by_thread.get(thread_id)
        if not existing:
            latest_by_thread[thread_id] = run
        else:
            existing_ts = _parse_iso((existing or {}).get("start_time"))
            if ts > existing_ts:
                latest_by_thread[thread_id] = run
                duplicates_excluded += 1
            else:
                duplicates_excluded += 1

    deduped = list(latest_by_thread.values())
    
    # Enrich runs with parsed user_id and lesson_id from thread_id
    enriched_runs = [enrich_run_with_thread_data(run) for run in deduped]

    logging.info("Total runs before deduplication: %s", len(runs))
    logging.info("Total runs after deduplication: %s", len(deduped))
    logging.info("Total excluded as older duplicates: %s", duplicates_excluded)
    logging.info("Enriched runs with user_id and lesson_id from thread_id")

    return enriched_runs
