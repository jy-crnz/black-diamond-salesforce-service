import asyncio
from datetime import datetime, timezone
from app.services.normalization_service import NormalizationService
from app.storage.minio_client import MinioStorageClient


async def run_demo():
    print("🚀 Booting Local Data-Lake Pipeline (Direct-to-MinIO)...\n" + "-" * 50)

    # 1. Initialize Services
    minio_client = MinioStorageClient()

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

    # 3. Compile and Upload to MinIO
    print("📦 Compiling Parquet memory stream...")
    parquet_stream = NormalizationService.convert_to_parquet_stream(
        dummy_records, id_column="Id"
    )

    print("☁️ Uploading directly to MinIO Data Lake...")
    minio_url = minio_client.upload_parquet_page(
        stream=parquet_stream,
        org_id="glynac-org-001",
        scan_id="scan-verification-004",
        object_type="Contact",
        page_num=1,
    )

    print(f"✅ Success! Data lake storage path: {minio_url}")
    print("-" * 50 + "\n✅ Demonstration Complete!")


if __name__ == "__main__":
    asyncio.run(run_demo())
