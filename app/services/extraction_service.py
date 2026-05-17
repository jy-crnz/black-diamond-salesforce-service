import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.clients.bulk_api_client import SalesforceBulkAPIClient
from app.services.normalization_service import NormalizationService
from app.storage.minio_client import MinioStorageClient
from app.storage.kafka_producer import SalesforceKafkaProducer

# NEW IMPORTS: PII Masking and Deduplication
from app.services.pii_service import PIIMaskingService
from app.services.deduplication_service import DeduplicationService

logger = logging.getLogger(__name__)


class SalesforceExtractionService:
    """
    Core data pipeline orchestrator. Manages the lifecycle of Salesforce Bulk 2.0
    query extractions, data transformations, cloud uploads, and message bus streaming.
    """

    def __init__(self):
        # The client manages its own authentication singleton internally.
        self.bulk_client = SalesforceBulkAPIClient()
        self.minio_client = MinioStorageClient()
        self.kafka_producer = SalesforceKafkaProducer()

        # NEW: Initialize the PII and Deduplication services
        self.pii_service = PIIMaskingService()
        self.dedupe_service = DeduplicationService()

        self._soql_templates = {
            "Contact": "SELECT Id, Name, Email, Phone, AccountId, OwnerId, CreatedDate, LastModifiedDate FROM Contact",
            "Account": "SELECT Id, Name, Type, Industry, BillingCity, BillingState, OwnerId, CreatedDate, LastModifiedDate FROM Account",
            "Opportunity": "SELECT Id, Name, AccountId, OwnerId, StageName, Amount, CloseDate, CreatedDate, LastModifiedDate FROM Opportunity",
            "Activity": "SELECT Id, Subject, Status, Priority, ActivityDate, OwnerId, CreatedDate, LastModifiedDate FROM Task",
            "Lead": "SELECT Id, Name, Company, Status, Email, OwnerId, CreatedDate, LastModifiedDate FROM Lead",
            "User": "SELECT Id, Name, Username, Email, IsActive, CreatedDate, LastModifiedDate FROM User WHERE IsActive = true",
            "CampaignMember": "SELECT Id, CampaignId, LeadId, ContactId, Status, CreatedDate, LastModifiedDate FROM CampaignMember",
        }

    def _build_soql_query(
        self, object_type: str, last_modified_after: Optional[str] = None
    ) -> str:
        base_query = self._soql_templates.get(object_type)
        if not base_query:
            raise ValueError(
                f"Unsupported extraction query schema targeting object: '{object_type}'"
            )

        if last_modified_after:
            if "WHERE" in base_query:
                return f"{base_query} AND LastModifiedDate >= {last_modified_after} ORDER BY LastModifiedDate ASC"
            else:
                return f"{base_query} WHERE LastModifiedDate >= {last_modified_after} ORDER BY LastModifiedDate ASC"

        return base_query

    async def run_bulk_extraction(
        self,
        scan_id: str,
        org_id: str,
        objects: List[str],
        filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Orchestrates an end-to-end extraction sequence across multiple requested target objects.
        Executes parallel extraction jobs and returns pipeline summary statistics.
        """
        logger.info(
            f"🚀 Ignition: Starting execution for Scan ID: {scan_id} | Org ID: {org_id}"
        )
        last_modified_after = filters.get("last_modified_after") if filters else None

        results_summary = {}
        total_extracted_records = 0

        for obj in objects:
            if obj not in self._soql_templates:
                logger.error(
                    f"Skipping extraction: Object '{obj}' is not supported by Glynac V1 spec."
                )
                continue

            try:
                # 1. Compile query and trigger Bulk 2.0 query slot
                soql = self._build_soql_query(obj, last_modified_after)
                sf_job_id = await self.bulk_client.create_query_job(soql)
                extracted_at = datetime.now(timezone.utc).isoformat()

                # 2. Block and adaptive-poll client layers until processing finishes
                logger.info(f"Polling status for {obj} job wrapper: {sf_job_id}...")
                job_status = await self.bulk_client.poll_until_complete(sf_job_id)

                if job_status.state != "JobComplete":
                    logger.error(
                        f"Salesforce Job {sf_job_id} aborted or failed: {job_status.error_message}"
                    )
                    results_summary[obj] = {
                        "status": "failed",
                        "error": job_status.error_message,
                    }
                    continue

                # 3. Pull data streams out of Salesforce memory pages
                page_num = 1
                obj_record_count = 0
                results_summary[obj] = {"sf_job_id": sf_job_id, "pages": []}

                results_iterator = self.bulk_client.iter_results(sf_job_id)

                if hasattr(results_iterator, "__aiter__"):
                    async for page_records in results_iterator:
                        if not page_records:
                            continue

                        # 4a. Deduplicate the raw CSV chunk before processing
                        unique_records = self.dedupe_service.deduplicate_records(
                            page_records
                        )

                        chunk_size = len(unique_records)
                        obj_record_count += chunk_size
                        total_extracted_records += chunk_size

                        # 4b. Write pristine raw data to the MinIO Data Lake
                        parquet_stream = NormalizationService.convert_to_parquet_stream(
                            unique_records, id_column="Id"
                        )
                        minio_url = self.minio_client.upload_parquet_page(
                            parquet_stream, org_id, scan_id, obj, page_num
                        )

                        # 4c. Apply PII Masking strictly for the event bus
                        masked_records = await self.pii_service.mask_batch(
                            unique_records
                        )

                        # 4d. Broadcast the redacted records to Kafka
                        for record in masked_records:
                            self.kafka_producer.publish_record(
                                record,
                                obj,
                                org_id,
                                scan_id,
                                sf_job_id,
                                page_num,
                                extracted_at,
                            )

                        results_summary[obj]["pages"].append(
                            {
                                "page_number": page_num,
                                "records_count": chunk_size,
                                "storage_path": minio_url,
                            }
                        )
                        page_num += 1
                else:
                    for page_records in results_iterator:
                        if not page_records:
                            continue

                        # 4a. Deduplicate the raw CSV chunk before processing
                        unique_records = self.dedupe_service.deduplicate_records(
                            page_records
                        )

                        chunk_size = len(unique_records)
                        obj_record_count += chunk_size
                        total_extracted_records += chunk_size

                        # 4b. Write pristine raw data to the MinIO Data Lake
                        parquet_stream = NormalizationService.convert_to_parquet_stream(
                            unique_records, id_column="Id"
                        )
                        minio_url = self.minio_client.upload_parquet_page(
                            parquet_stream, org_id, scan_id, obj, page_num
                        )

                        # 4c. Apply PII Masking strictly for the event bus
                        masked_records = await self.pii_service.mask_batch(
                            unique_records
                        )

                        # 4d. Broadcast the redacted records to Kafka
                        for record in masked_records:
                            self.kafka_producer.publish_record(
                                record,
                                obj,
                                org_id,
                                scan_id,
                                sf_job_id,
                                page_num,
                                extracted_at,
                            )

                        results_summary[obj]["pages"].append(
                            {
                                "page_number": page_num,
                                "records_count": chunk_size,
                                "storage_path": minio_url,
                            }
                        )
                        page_num += 1

                # 7. Perform best-effort remote resource cleanup
                await self.bulk_client.delete_job(sf_job_id)

                results_summary[obj]["status"] = "completed"
                results_summary[obj]["total_records"] = obj_record_count
                logger.info(
                    f"✅ Finished extraction sync for {obj}. Total records processed: {obj_record_count}"
                )

            except Exception as e:
                logger.exception(
                    f"Unhandled processing collapse executing pipeline synchronization for '{obj}': {e}"
                )
                results_summary[obj] = {"status": "error", "message": str(e)}

        # 8. Finalize outstanding events before concluding transaction
        self.kafka_producer.flush_bus()

        logger.info(
            f"🏁 Sync operation complete. Grand total records exported: {total_extracted_records}"
        )
        return {
            "scan_id": scan_id,
            "org_id": org_id,
            "status": "success",
            "totals": {
                "records_extracted": total_extracted_records,
                "objects_processed": len(objects),
            },
            "details": results_summary,
        }
