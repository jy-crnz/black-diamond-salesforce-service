import logging
import os
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class PIIMaskingService:
    """
    Manages data anonymization before broadcasting to downstream consumers.
    Adheres to the project specifications for PII redaction.
    """

    def __init__(self):
        # Safely pull from environment, defaulting to False if not set
        self.enabled = str(os.getenv("PII_MASKING_ENABLED", "false")).lower() == "true"
        self.service_url = os.getenv("PII_SERVICE_URL", "http://localhost:8100")

        # The specific fields required for redaction
        self.target_fields = {"Name", "Email", "Phone"}

    async def mask_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Masks PII fields in a batch of records.
        Uses a non-mutating copy approach to protect the raw memory stream.
        """
        if not self.enabled:
            return records

        masked_batch = []
        for record in records:
            # Create a shallow copy so we don't accidentally mutate the raw data
            # that is destined for the MinIO Parquet conversion
            safe_record = record.copy()

            for field in self.target_fields:
                if field in safe_record and safe_record[field]:
                    safe_record[field] = "[MASKED]"

            masked_batch.append(safe_record)

        return masked_batch
