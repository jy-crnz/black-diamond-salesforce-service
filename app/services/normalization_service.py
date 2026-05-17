import io
import logging
from typing import Any, Dict, List
import pandas as pd

logger = logging.getLogger(__name__)


class NormalizationService:
    """
    Dedicated extraction engine converter. Transforms unstructured raw rows
    into highly optimized, Snappy-compressed columnar Parquet streams.
    """

    @staticmethod
    def convert_to_parquet_stream(
        records: List[Dict[str, Any]], id_column: str = "Id"
    ) -> io.BytesIO:
        """
        Ingests a list of record dictionaries, normalizes system datatypes,
        and generates a seekable binary Parquet stream.
        """
        if not records:
            logger.warning("Empty record collection passed to normalization gateway.")
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(records)

        # FIXED: Removed the local static call to DeduplicationService.deduplicate_records.
        # Deduplication is now cleanly managed upstream by the extraction orchestrator
        # to ensure unified handling of raw data records across the entire pipeline.

        # 2. Structural data type optimization
        # Ensures timezone and null strings map uniformly across our parquet schemas
        for column in df.columns:
            if column.endswith("Date") or column == "SystemModstamp":
                df[column] = df[column].astype(str)

        # 3. Compile structural data straight into a Snappy binary buffer stream
        parquet_buffer = io.BytesIO()
        try:
            df.to_parquet(
                parquet_buffer, engine="pyarrow", index=False, compression="snappy"
            )
            parquet_buffer.seek(0)  # Re-point stream cursor to ignition boundary
            logger.info(
                f"Normalization complete: Compiled {len(df)} records into "
                f"a high-performance Snappy-compressed Parquet memory stream."
            )
        except Exception as e:
            logger.critical(
                f"Fatal error compiling dataset to Parquet byte format: {e}"
            )
            raise e

        return parquet_buffer
