"""File management utilities for LangSmith exports."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


def write_runs_file(runs: List[Dict[str, Any]], output_dir: str) -> str:
    """
    Write runs to a timestamped JSON file.
    
    Args:
        runs: List of run dictionaries to save
        output_dir: Directory to save the file in
        
    Returns:
        Path to the created file
    """
    tz = ZoneInfo("Asia/Jerusalem")
    now_local = datetime.now(tz)
    # Match n8n's format 'yyyy-MM-dd-hh-mm' where 'hh' is 12-hour clock
    file_name = now_local.strftime("langchain-runs-%Y-%m-%d-%I-%M.txt")
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, file_name)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(runs, f, ensure_ascii=False, indent=2)

    logging.info("Wrote %s runs to %s", len(runs), file_path)
    return file_path
