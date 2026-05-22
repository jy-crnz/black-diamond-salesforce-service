import asyncio
from datetime import datetime, timezone
from app.services.pii_service import PIIMaskingService
from app.services.normalization_service import NormalizationService
from app.storage.minio_client import MinioStorageClient
from app.storage.kafka_producer import SalesforceKafkaProducer


async def run_demo():
    print("🚀 Booting Local Demonstration Pipeline...\n" + "-" * 50)

    # 1. Initialize Services
    pii_service = PIIMaskingService()
    minio_client = MinioStorageClient()
    kafka_producer = SalesforceKafkaProducer()

    # 2. Create Dummy Salesforce Data
    dummy_records = [
        {
            "Id": "003A000001abc123",
            "Name": "Alice Walker",
            "Email": "alice@example.com",
            "Phone": "555-0101",
            "LastModifiedDate": datetime.now(timezone.utc).isoformat(),
        },
        {
            "Id": "003A000001abc124",
            "Name": "Bob Builder",
            "Email": "bob@example.com",
            "Phone": "555-0202",
            "LastModifiedDate": datetime.now(timezone.utc).isoformat(),
        },
        {
            "Id": "003A000001abc125",
            "Name": "Charlie Chaplin",
            "Email": "charlie@example.com",
            "Phone": "555-0303",
            "LastModifiedDate": datetime.now(timezone.utc).isoformat(),
        },
    ]

    # 3. MinIO Parquet Normalization
    print("📦 Compiling Parquet memory stream...")
    parquet_stream = NormalizationService.convert_to_parquet_stream(dummy_records)
    minio_client.upload_parquet_page(
        stream=parquet_stream,
        org_id="glynac-org-001",
        scan_id="scan-verification-004",
        object_type="Contact",
        page_num=1,
    )

    # 4. PII Masking & Kafka Streaming
    print("🛡️ Applying PII Masking Shield...")
    masked_records = await pii_service.mask_batch(dummy_records)

    print("📡 Streaming to Apache Kafka...")
    for record in masked_records:
        kafka_producer.publish_record(
            record=record,
            object_type="Contact",
            org_id="glynac-org-001",
            scan_id="scan-verification-004",
            sf_job_id="demo-job-id",
            page_num=1,
            extracted_at=datetime.now(timezone.utc).isoformat(),
        )

    kafka_producer.flush_bus()
    print("-" * 50 + "\n✅ Demonstration Complete!")


if __name__ == "__main__":
    asyncio.run(run_demo())
