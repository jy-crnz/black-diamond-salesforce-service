import csv
import io
import asyncio
import logging
import httpx
from typing import AsyncGenerator, Optional, List, Dict
from pydantic import BaseModel
from app.core.config import settings
from app.auth.salesforce_auth import token_manager

logger = logging.getLogger(__name__)


class BulkJobResult(BaseModel):
    job_id: str
    state: str
    records_processed: int
    records_failed: int
    error_message: Optional[str] = None


class SalesforceBulkAPIClient:
    """
    Asynchronous wrapper for Salesforce Bulk API 2.0.
    Handles the strict 4-step job lifecycle: Create, Poll, Paginate, Cleanup.
    """

    # Adaptive polling: start fast (5s), slow down to save API limits (up to 120s)
    POLL_INTERVALS = [5, 5, 5, 5, 5, 15, 15, 15, 60, 60, 120]

    def __init__(self, timeout: float = 30.0):
        self._timeout = timeout

    async def _get_headers_and_base_url(self) -> tuple[dict, str]:
        """Injects the dynamic token into headers before every request."""
        token, instance_url = await token_manager.get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        base_url = f"{instance_url}/services/data/{settings.SF_API_VERSION}/jobs/query"
        return headers, base_url

    async def create_query_job(self, soql: str) -> str:
        """Step 1: Creates a Bulk API 2.0 query job and returns the job_id."""
        headers, base_url = await self._get_headers_and_base_url()
        payload = {
            "operation": "query",
            "query": soql,
            "contentType": "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding": "LF",
        }

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(base_url, json=payload, headers=headers)
            resp.raise_for_status()

            job_id = resp.json()["id"]
            logger.info(f"Created Bulk API query job: {job_id}")
            return job_id

    async def poll_until_complete(self, job_id: str) -> BulkJobResult:
        """Step 2: Blocks asynchronously until the job reaches a terminal state."""
        headers, base_url = await self._get_headers_and_base_url()
        url = f"{base_url}/{job_id}"
        intervals = iter(self.POLL_INTERVALS)

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            while True:
                resp = await client.get(url, headers=headers)
                resp.raise_for_status()
                data = resp.json()
                state = data["state"]

                if state in ("JobComplete", "Failed", "Aborted"):
                    return BulkJobResult(
                        job_id=job_id,
                        state=state,
                        records_processed=data.get("numberRecordsProcessed", 0),
                        records_failed=data.get("numberRecordsFailed", 0),
                        error_message=data.get("errorMessage"),
                    )

                # Get the next interval, default to 120s if we run out of the list
                delay = next(intervals, 120)
                logger.debug(f"Job {job_id} state={state}, waiting {delay}s")
                await asyncio.sleep(delay)

    async def iter_results(
        self, job_id: str, page_size: int = 50000
    ) -> AsyncGenerator[List[Dict], None]:
        """Step 3: Yields pages of records, handling Sforce-Locator pagination transparently."""
        headers, base_url = await self._get_headers_and_base_url()
        # Override headers for CSV download
        headers.pop("Content-Type")
        locator = None
        page_num = 0

        async with httpx.AsyncClient(timeout=self._timeout * 6) as client:
            while True:
                url = f"{base_url}/{job_id}/results?maxRecords={page_size}"
                if locator:
                    url += f"&locator={locator}"

                resp = await client.get(url, headers=headers)
                resp.raise_for_status()

                # Parse CSV body synchronously (it's safe enough for 50k chunks in memory)
                text = resp.text
                reader = csv.DictReader(io.StringIO(text))
                records = list(reader)
                page_num += 1
                logger.info(f"Job {job_id} page {page_num}: {len(records)} records")

                yield records

                locator = resp.headers.get("Sforce-Locator")
                if not locator or locator == "null":
                    break

    async def delete_job(self, job_id: str) -> None:
        """Step 4: Deletes a completed job from Salesforce (Cleanup)."""
        headers, base_url = await self._get_headers_and_base_url()
        url = f"{base_url}/{job_id}"

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            try:
                resp = await client.delete(url, headers=headers)
                resp.raise_for_status()
                logger.info(f"Deleted Bulk API job: {job_id}")
            except Exception as e:
                logger.warning(f"Failed to delete job {job_id}: {e}")
