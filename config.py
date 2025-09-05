"""Configuration management for LangSmith export tool."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class Config:
    """Configuration for LangSmith export."""
    
    # LangSmith API settings
    langsmith_api_key: str
    session_ids: List[str]
    hours_window: int = 24
    filter_name: str = "tutor"
    limit: int = 100
    
    # S3 settings
    s3_bucket_name: str = ""
    aws_region: str = "us-east-1"
    
    # MongoDB settings
    mongo_connection_string: str = ""
    mongo_database_name: str = ""
    mongo_collection_name: str = ""
    
    # Output settings
    output_dir: str = "langsmith-exports"
    
    # Logging
    log_level: str = "INFO"
    
    @classmethod
    def from_env(cls) -> "Config":
        """Create configuration from environment variables."""
        api_key = os.getenv("LANGSMITH_API_KEY", "").strip()
        if not api_key:
            raise ValueError("LANGSMITH_API_KEY is required")
        
        session_ids_env = os.getenv(
            "LS_SESSION_IDS", "8aa48f29-844f-40cf-8062-301e9fc4f500"
        ).strip()
        session_ids = [s.strip() for s in session_ids_env.split(",") if s.strip()]
        
        return cls(
            langsmith_api_key=api_key,
            session_ids=session_ids,
            hours_window=int(os.getenv("LS_HOURS_WINDOW", "24")),
            filter_name=os.getenv("LS_FILTER_NAME", "tutor").strip(),
            s3_bucket_name=os.getenv("S3_BUCKET_NAME", "").strip(),
            aws_region=os.getenv("AWS_REGION", "us-east-1").strip(),
            mongo_connection_string=os.getenv("MONGO_CONNECTION_STRING", "").strip(),
            mongo_database_name=os.getenv("MONGO_DATABASE_NAME", "").strip(),
            mongo_collection_name=os.getenv("MONGO_COLLECTION_NAME", "").strip(),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )
