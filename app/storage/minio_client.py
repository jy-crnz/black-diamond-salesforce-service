import io
import logging
from minio import Minio
from app.core.config import settings

logger = logging.getLogger(__name__)


class MinioStorageClient:
    """
    Handles enterprise object-storage interactions with S3-compatible clusters.
    Ensures safe bucket provisioning and manages multi-part data chunk uploads.
    """

    def __init__(self):
        self.enabled = settings.MINIO_ENABLED
        self.bucket_name = settings.MINIO_BUCKET
        self.client = None

        if not self.enabled:
            logger.warning(
                "MinIO Storage is globally disabled in configuration settings. Uploads will be skipped."
            )
            return

        try:
            # Safely unpack the client endpoints and secret tokens
            self.client = Minio(
                endpoint=settings.MINIO_ENDPOINT,
                access_key=settings.MINIO_ACCESS_KEY,
                secret_key=settings.MINIO_SECRET_KEY.get_secret_value(),
                secure=settings.MINIO_SECURE,
            )
            self._ensure_bucket_exists()
        except Exception as e:
            logger.critical(
                f"Fatal failure connecting to MinIO cluster at {settings.MINIO_ENDPOINT}: {e}"
            )
            raise e

    def _ensure_bucket_exists(self) -> None:
        """Verifies target bucket presence; automates bucket provisioning if absent."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                logger.info(
                    f"Target storage bucket '{self.bucket_name}' not found. Creating it..."
                )
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Successfully provisioned bucket '{self.bucket_name}'.")
        except Exception as e:
            logger.error(
                f"Failed to verify or provision storage bucket '{self.bucket_name}': {e}"
            )
            raise e

    def upload_parquet_page(
        self,
        stream: io.BytesIO,
        org_id: str,
        scan_id: str,
        object_type: str,
        page_num: int,
    ) -> str:
        """
        Uploads an in-memory Parquet stream chunk to a strict object folder hierarchy.
        Target Layout: {bucket}/{org_id}/{scan_id}/{object_type}/page_{page_num:03d}.parquet
        """
        if not self.enabled or not self.client:
            logger.info(
                "Skipping upload: MinIO storage client is inactive or disabled."
            )
            return ""

        # Format page numbers with padded zeros (e.g., page_001.parquet) for alphabetical sorting
        destination_path = (
            f"{org_id}/{scan_id}/{object_type.lower()}/page_{page_num:03d}.parquet"
        )

        # Calculate size of the stream buffer
        stream.seek(0, io.SEEK_END)
        stream_length = stream.tell()
        stream.seek(0)  # Reset cursor before feeding to client

        try:
            logger.info(
                f"Initiating MinIO chunk upload to: s3://{self.bucket_name}/{destination_path} ({stream_length} bytes)"
            )
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=destination_path,
                data=stream,
                length=stream_length,
                content_type="application/octet-stream",
            )
            logger.info(
                f"Successfully finalized upload for {object_type} page {page_num}."
            )
            return f"s3://{self.bucket_name}/{destination_path}"
        except Exception as e:
            logger.error(
                f"Failed uploading Parquet data block to location '{destination_path}': {e}"
            )
            raise e
