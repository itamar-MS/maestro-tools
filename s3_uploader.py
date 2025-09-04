"""S3 upload functionality for LangSmith export files."""
from __future__ import annotations

import logging
import os
from typing import Optional

from config import Config


class S3Uploader:
    """Handles uploading files to AWS S3."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def upload_file(self, file_path: str) -> Optional[str]:
        """
        Upload file to S3 bucket and return the S3 URL if successful.
        
        Args:
            file_path: Local path to the file to upload
            
        Returns:
            S3 URL if successful, None otherwise
        """
        if not self.config.s3_bucket_name:
            logging.info("No S3 bucket configured. File saved locally only.")
            return None
        
        try:
            import boto3  # type: ignore
            from botocore.exceptions import ClientError  # type: ignore
        except ImportError:
            logging.error("boto3 not installed. Install with: pip install boto3")
            return None

        try:
            s3_client = boto3.client("s3", region_name=self.config.aws_region)
            file_name = os.path.basename(file_path)
            
            s3_client.upload_file(file_path, self.config.s3_bucket_name, file_name)
            s3_url = f"s3://{self.config.s3_bucket_name}/{file_name}"
            logging.info("Uploaded to S3: %s", s3_url)
            return s3_url
            
        except ClientError as e:
            logging.error("❌ Failed to upload to S3: %s", e)
            return None
        except Exception as e:
            logging.error("💥 Unexpected error during S3 upload: %s", e)
            return None
