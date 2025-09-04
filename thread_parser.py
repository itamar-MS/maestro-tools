"""Thread ID parsing utilities for extracting user_id and lesson_id."""
from __future__ import annotations

import logging
from typing import Optional, Tuple


def parse_thread_id(thread_id: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse thread_id to extract user_id and lesson_id.
    
    Expected format: <user-id>-<lesson-id>
    
    Args:
        thread_id: Thread ID string from LangSmith
        
    Returns:
        Tuple of (user_id, lesson_id) or (None, None) if parsing fails
    """
    if not thread_id or not isinstance(thread_id, str):
        return None, None
    
    # Split by the last hyphen to handle user IDs that might contain hyphens
    parts = thread_id.rsplit("-", 1)
    
    if len(parts) != 2:
        logging.debug("🔍 Could not parse thread_id '%s' - expected format: user-id-lesson-id", thread_id)
        return None, None
    
    user_id, lesson_id = parts
    
    # Basic validation
    if not user_id or not lesson_id:
        logging.debug("🔍 Invalid thread_id parts in '%s' - user_id: '%s', lesson_id: '%s'", 
                     thread_id, user_id, lesson_id)
        return None, None
    
    return user_id.strip(), lesson_id.strip()


def enrich_run_with_thread_data(run: dict) -> dict:
    """
    Enrich a run dictionary with parsed user_id and lesson_id from thread_id.
    
    Args:
        run: Run dictionary from LangSmith API
        
    Returns:
        Enhanced run dictionary with user_id and lesson_id fields
    """
    if not isinstance(run, dict):
        return run
    
    # Create a copy to avoid modifying the original
    enriched_run = run.copy()
    
    thread_id = run.get("thread_id")
    user_id, lesson_id = parse_thread_id(thread_id)
    
    # Add parsed fields
    enriched_run["user_id"] = user_id
    enriched_run["lesson_id"] = lesson_id
    
    return enriched_run
