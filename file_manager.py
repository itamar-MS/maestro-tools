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


def write_runs_files(runs: List[Dict[str, Any]], output_dir: str) -> tuple[str, str]:
    """
    Write runs to two timestamped JSON files: one with outputs, one without.
    
    Args:
        runs: List of run dictionaries to save
        output_dir: Directory to save the files in
        
    Returns:
        Tuple of (full_file_path, summary_file_path)
    """
    tz = ZoneInfo("Asia/Jerusalem")
    now_local = datetime.now(tz)
    timestamp = now_local.strftime("%Y-%m-%d-%H-%M")
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Full file (with outputs)
    full_file_name = f"langchain-runs-full-{timestamp}.txt"
    full_file_path = os.path.join(output_dir, full_file_name)
    
    # Summary file (without outputs)
    summary_file_name = f"langchain-runs-summary-{timestamp}.txt"
    summary_file_path = os.path.join(output_dir, summary_file_name)
    
    # Create summary runs without outputs field
    summary_runs = []
    for run in runs:
        if isinstance(run, dict):
            summary_run = {k: v for k, v in run.items() if k != "outputs"}
            summary_runs.append(summary_run)
        else:
            summary_runs.append(run)
    
    # Write full file
    with open(full_file_path, "w", encoding="utf-8") as f:
        json.dump(runs, f, ensure_ascii=False, indent=2)
    
    # Write summary file
    with open(summary_file_path, "w", encoding="utf-8") as f:
        json.dump(summary_runs, f, ensure_ascii=False, indent=2)
    
    logging.info("Wrote %s runs to full file: %s", len(runs), full_file_path)
    logging.info("Wrote %s runs to summary file: %s", len(summary_runs), summary_file_path)
    
    return full_file_path, summary_file_path


def write_runs_file(runs: List[Dict[str, Any]], output_dir: str) -> str:
    """
    Legacy function for backward compatibility.
    Write runs to a timestamped JSON file.
    
    Args:
        runs: List of run dictionaries to save
        output_dir: Directory to save the file in
        
    Returns:
        Path to the created file
    """
    full_path, _ = write_runs_files(runs, output_dir)
    return full_path
