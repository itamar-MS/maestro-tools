"""Statistics calculation for LangSmith export data."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Set


def calculate_export_stats(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate statistics from the exported runs.
    
    Args:
        runs: List of run dictionaries from LangSmith
        
    Returns:
        Dictionary with statistics
    """
    if not runs:
        return {
            "total_runs": 0,
            "conversations": 0,
            "unique_users": 0,
            "unique_lessons": 0,
            "thread_ids": set(),
            "user_ids": set(),
            "lesson_ids": set(),
        }
    
    thread_ids: Set[str] = set()
    user_ids: Set[str] = set()
    lesson_ids: Set[str] = set()
    
    for run in runs:
        if not isinstance(run, dict):
            continue
            
        # Count unique thread_ids (conversations)
        thread_id = run.get("thread_id")
        if thread_id:
            thread_ids.add(str(thread_id))
        
        # Count unique user_ids (parsed from thread_id)
        user_id = run.get("user_id")
        if user_id:
            user_ids.add(str(user_id))
            
        # Count unique lesson_ids (parsed from thread_id)
        lesson_id = run.get("lesson_id")
        if lesson_id:
            lesson_ids.add(str(lesson_id))
    
    stats = {
        "total_runs": len(runs),
        "conversations": len(thread_ids),
        "unique_users": len(user_ids),
        "unique_lessons": len(lesson_ids),
        "thread_ids": thread_ids,
        "user_ids": user_ids,
        "lesson_ids": lesson_ids,
    }
    
    return stats


def log_export_stats(stats: Dict[str, Any]) -> None:
    """Log export statistics in a formatted way with emojis."""
    logging.info("📊 " + "=" * 48)
    logging.info("📈 EXPORT STATISTICS")
    logging.info("📊 " + "=" * 48)
    logging.info("💬 Conversations (unique thread_ids): %s", stats["conversations"])
    logging.info("👥 Unique users (parsed from thread_ids): %s", stats["unique_users"])
    logging.info("📚 Unique lessons (parsed from thread_ids): %s", stats["unique_lessons"])
    logging.info("✅ " + "=" * 48)
