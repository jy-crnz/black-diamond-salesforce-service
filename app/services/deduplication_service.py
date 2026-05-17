import logging
import pandas as pd
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class DeduplicationService:
    """
    Ensures data ingestion idempotency by removing duplicate records
    within batch extraction chunks.
    """

    def deduplicate_records(
        self,
        records: List[Dict[str, Any]],
        id_column: str = "Id",
        sort_column: str = "LastModifiedDate",
    ) -> List[Dict[str, Any]]:
        """
        Removes duplicates from a list of dictionaries based on the id_column.
        Retains the freshest record if sort_column is present.
        """
        if not records:
            return []

        try:
            # 1. Safely convert the raw list of dictionaries into a Pandas DataFrame
            df = pd.DataFrame(records)

            if id_column not in df.columns:
                logger.warning(
                    f"Deduplication bypassed: Column '{id_column}' not found in dataset."
                )
                return records

            original_count = len(df)

            # 2. Sort by LastModifiedDate descending (if it exists) so we keep the newest data
            if sort_column in df.columns:
                df = df.sort_values(by=sort_column, ascending=False)

            # 3. Drop the duplicates, keeping the first (newest) row it encounters
            df = df.drop_duplicates(subset=[id_column], keep="first")

            deduped_count = len(df)
            if original_count > deduped_count:
                logger.info(
                    f"Purged {original_count - deduped_count} duplicate records from chunk."
                )

            # 4. Convert back to a pure list of dictionaries for the rest of the pipeline
            # Use 'records' orient to match the exact shape the pipeline expects
            return df.to_dict(orient="records")

        except Exception as e:
            logger.error(
                f"Deduplication engine failed, bypassing constraint. Error: {str(e)}"
            )
            return records
