"""MongoDB upload functionality for LangSmith export conversations."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from config import Config

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


class MongoUploader:
    """Handles uploading conversations to MongoDB."""
    
    def __init__(self, config: Config):
        self.config = config
        self.client = None
        self.db = None
        self.collection = None
    
    def connect(self) -> bool:
        """
        Connect to MongoDB cluster.
        
        Returns:
            True if connection successful, False otherwise
        """
        if not self.config.mongo_connection_string:
            logging.info("No MongoDB connection string configured. Skipping MongoDB upload.")
            return False
        
        if not self.config.mongo_database_name:
            logging.error("MONGO_DATABASE_NAME environment variable is required for MongoDB upload.")
            return False
        
        if not self.config.mongo_collection_name:
            logging.error("MONGO_COLLECTION_NAME environment variable is required for MongoDB upload.")
            return False
        
        try:
            import pymongo  # type: ignore
        except ImportError:
            logging.error("pymongo not installed. Install with: pip install pymongo")
            return False
        
        try:
            self.client = pymongo.MongoClient(self.config.mongo_connection_string)
            # Test connection
            self.client.admin.command('ping')
            
            self.db = self.client[self.config.mongo_database_name]
            self.collection = self.db[self.config.mongo_collection_name]
            
            # Create unique index on thread_id if it doesn't exist
            self.collection.create_index("thread_id", unique=True)
            
            logging.info("Connected to MongoDB successfully")
            return True
            
        except Exception as e:
            logging.error("❌ Failed to connect to MongoDB: %s", e)
            return False
    
    def upload_conversations(self, runs: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Upload conversations to MongoDB with upsert behavior.
        
        Args:
            runs: List of processed runs with conversation data
            
        Returns:
            Dictionary with counts: {"inserted": int, "updated": int, "errors": int}
        """
        if not self.connect():
            return {"inserted": 0, "updated": 0, "errors": 0}
        
        stats = {"inserted": 0, "updated": 0, "errors": 0}
        current_time = datetime.now(ZoneInfo("UTC")).isoformat()
        
        for run in runs:
            if not isinstance(run, dict):
                stats["errors"] += 1
                continue
            
            thread_id = run.get("thread_id")
            if not thread_id:
                logging.warning("Skipping run without thread_id")
                stats["errors"] += 1
                continue
            
            try:
                # Prepare document for MongoDB
                mongo_doc = self._prepare_document(run, current_time)
                
                # Use upsert to insert or update based on thread_id
                result = self.collection.replace_one(
                    {"thread_id": thread_id},
                    mongo_doc,
                    upsert=True
                )
                
                if result.upserted_id:
                    stats["inserted"] += 1
                    logging.debug("Inserted new conversation: %s", thread_id)
                else:
                    stats["updated"] += 1
                    logging.debug("Updated existing conversation: %s", thread_id)
                    
            except Exception as e:
                logging.error("Failed to upload conversation %s: %s", thread_id, e)
                stats["errors"] += 1
        
        self._log_upload_stats(stats)
        return stats
    
    def _prepare_document(self, run: Dict[str, Any], current_time: str) -> Dict[str, Any]:
        """
        Prepare a run document for MongoDB storage.
        
        Args:
            run: Run data from LangSmith
            current_time: Current timestamp for tracking updates
            
        Returns:
            Document ready for MongoDB insertion
        """
        # Create a copy and add MongoDB metadata
        doc = run.copy()
        
        # Add MongoDB-specific fields
        doc["mongo_updated_at"] = current_time
        doc["mongo_created_at"] = doc.get("mongo_created_at", current_time)  # Preserve original creation time
        
        # Ensure thread_id is present and clean
        doc["thread_id"] = str(doc.get("thread_id", ""))
        
        return doc
    
    def _log_upload_stats(self, stats: Dict[str, int]) -> None:
        """Log upload statistics."""
        total_processed = stats["inserted"] + stats["updated"] + stats["errors"]
        
        logging.info("📊 MongoDB Upload Statistics:")
        logging.info("  📝 Total conversations processed: %d", total_processed)
        logging.info("  ✨ New conversations inserted: %d", stats["inserted"])
        logging.info("  🔄 Existing conversations updated: %d", stats["updated"])
        logging.info("  ❌ Errors encountered: %d", stats["errors"])
        
        if stats["errors"] > 0:
            logging.warning("⚠️  Some conversations failed to upload. Check logs for details.")
    
    def close(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logging.debug("MongoDB connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
