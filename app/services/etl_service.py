import logging
from app.clients.bulk_api_client import SalesforceBulkAPIClient
from app.repositories.salesforce_repository import SalesforceRepository
from app.core.database import async_session_maker

logger = logging.getLogger(__name__)


async def execute_salesforce_sync() -> dict:
    """
    Core ETL Service: Extracts live Account data from Salesforce,
    transforms the schema, and upserts into the local PostgreSQL database.
    """
    sf_client = SalesforceBulkAPIClient()
    total_upserted = 0
    job_id = None

    try:
        async with async_session_maker() as session:
            repository = SalesforceRepository(session)

            # 1. EXTRACT
            logger.info("Starting Salesforce Extract...")
            soql = "SELECT Id, Name, Type, Industry FROM Account"
            job_id = await sf_client.create_query_job(soql)

            job_result = await sf_client.poll_until_complete(job_id)
            if job_result.state != "JobComplete":
                raise Exception(f"Salesforce Job failed with state: {job_result.state}")

            # 2. TRANSFORM & LOAD
            logger.info("Starting Transform and Load phase...")
            async for record_page in sf_client.iter_results(job_id, page_size=2000):
                transformed_batch = []
                for record in record_page:
                    transformed_batch.append(
                        {
                            "salesforce_id": record.get("Id"),
                            "name": record.get("Name"),
                            "type": record.get("Type"),
                            "industry": record.get("Industry"),
                        }
                    )

                batch_count = await repository.bulk_upsert_accounts(transformed_batch)
                total_upserted += batch_count

            # Commit the transaction safely
            await session.commit()

            return {
                "status": "success",
                "records_processed": total_upserted,
                "salesforce_job_id": job_id,
            }

    except Exception as e:
        logger.error(f"ETL Sync failed: {str(e)}")
        raise e

    finally:
        # 3. CLEANUP (Always runs, even if an error occurs above)
        if job_id:
            logger.info(f"Cleaning up Salesforce Job: {job_id}")
            await sf_client.delete_job(job_id)
