#!/usr/bin/env python3
"""
Main entry point for LangSmith export tool.

This script exports LangSmith runs to timestamped files and optionally uploads to S3.
It replicates the provided n8n workflow functionality.

Usage:
    python main.py [options]

Examples:
    python main.py                           # Default: JSON format, last 24 hours (no S3)
    python main.py --output json             # JSON format only (explicit)
    python main.py --output s3               # JSON format + S3 upload
    python main.py --output json,s3          # JSON format + S3 upload (same as above)
    python main.py --hours 12               # Last 12 hours only
    python main.py --debug 50               # Debug mode: limit to 50 runs

Environment variables are loaded from .env file automatically.
See config.py for all available configuration options.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from typing import List

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

from config import Config
from langsmith_client import LangSmithClient
from file_manager import write_runs_files
from s3_uploader import S3Uploader
from stats_calculator import calculate_export_stats, log_export_stats


def setup_logging(log_level: str) -> None:
    """Configure logging with the specified level."""
    level = getattr(logging, log_level, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Export LangSmith runs to files and optionally upload to S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                           # Default: JSON format, last 24 hours (no S3)
  %(prog)s --output json             # JSON format only (explicit)
  %(prog)s --output s3               # JSON format + S3 upload
  %(prog)s --output json,s3          # JSON format + S3 upload (same as above)
  %(prog)s --hours 12                # Last 12 hours only
  %(prog)s --debug 50                # Debug mode: limit to 50 runs
        """
    )
    
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="json",
        help="Output options (comma-separated): json, s3. Default: json"
    )
    
    
    parser.add_argument(
        "--debug",
        type=int,
        metavar="N",
        help="Debug mode: limit to N runs (e.g., --debug 10)"
    )
    
    parser.add_argument(
        "--hours",
        type=float,
        default=24.0,
        metavar="N",
        help="Time window in hours to fetch runs (default: 24)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Override log level from environment"
    )
    
    return parser.parse_args()


def parse_output_options(output_arg: str) -> bool:
    """
    Parse output argument to determine S3 upload flag.
    
    Returns:
        upload_to_s3 boolean
    """
    options = [opt.strip().lower() for opt in output_arg.split(",")]
    
    upload_to_s3 = False
    
    for opt in options:
        if opt == "s3":
            upload_to_s3 = True
        elif opt == "json":
            # JSON is always the default format
            continue
        else:
            logging.warning("⚠️ Unknown output option '%s', ignoring", opt)
    
    return upload_to_s3


def main() -> int:
    """Main entry point."""
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Load configuration
        config = Config.from_env()
        
        # Debug mode is CLI-only now
        debug_limit = args.debug
        
        log_level = args.log_level or config.log_level
        setup_logging(log_level)
        
        # Parse output options
        upload_to_s3_cli = parse_output_options(args.output)
        
        # Determine S3 upload: Only upload if explicitly requested
        upload_to_s3 = upload_to_s3_cli
        
        logging.info("🚀 Starting LangSmith export")
        logging.info("Output format: JSON")
        logging.info("S3 upload: %s", "enabled" if upload_to_s3 else "disabled")
        
        # Calculate time window
        tz_utc = ZoneInfo("UTC")
        end_time = datetime.now(tz=tz_utc)
        start_time = end_time - timedelta(hours=args.hours)
        
        logging.info(
            "Querying LangSmith runs with config: %s",
            {
                "session_ids": config.session_ids,
                "hours_window": args.hours,
                "filter_name": config.filter_name,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "debug_limit": debug_limit,
            },
        )
        
        # Fetch runs from LangSmith (includes deduplication and enrichment)
        client = LangSmithClient(config)
        deduped_runs = client.fetch_all_runs(start_time=start_time, end_time=end_time, debug_limit=debug_limit)
        
        # Write JSON files (full and summary)
        full_file_path, summary_file_path = write_runs_files(deduped_runs, output_dir=config.output_dir)
        
        # Optional S3 upload
        if upload_to_s3:
            uploader = S3Uploader(config)
            
            # Upload both files
            full_s3_url = uploader.upload_file(full_file_path)
            summary_s3_url = uploader.upload_file(summary_file_path)
            
            if full_s3_url and summary_s3_url:
                logging.info("✅ Both files uploaded to S3 successfully.")
            elif full_s3_url or summary_s3_url:
                logging.warning("⚠️ Only one file uploaded to S3 successfully, but local files saved.")
            else:
                logging.warning("⚠️ S3 upload failed for both files, but local files saved.")
        
        # Calculate and display statistics
        stats = calculate_export_stats(deduped_runs)
        log_export_stats(stats)
        
        logging.info("✅ Export completed successfully.")
        return 0
        
    except ValueError as e:
        logging.error("❌ Configuration error: %s", e)
        return 1
    except Exception as e:
        logging.error("💥 Unexpected error: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
