from fastapi import APIRouter, Depends, HTTPException
from app.services.etl_service import execute_salesforce_sync
from app.core.security import verify_hmac  # <-- Make sure this is imported

router = APIRouter(prefix="/sync", tags=["Synchronization"])


@router.post(
    "/salesforce",
    summary="Trigger Salesforce Account Sync",
    dependencies=[Depends(verify_hmac)],  # Locked down tight!
)
async def trigger_salesforce_sync():
    """
    Webhook endpoint to trigger the Salesforce-to-Postgres ETL pipeline.
    Protected by HMAC SHA-256 signature validation.
    """
    try:
        result = await execute_salesforce_sync()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")