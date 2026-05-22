import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, status, Depends, Response
from pydantic import BaseModel, Field

from app.services.extraction_service import SalesforceExtractionService
from app.services.maintenance_service import MaintenanceService

# Import your secure HMAC verification function
from app.core.security import verify_hmac

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/sync", tags=["Synchronization"])
extraction_service = SalesforceExtractionService()
maintenance_service = MaintenanceService()


# ==========================================================================
# 📋 Strict Input/Output Boundary Schemas
# ==========================================================================
class DestinationOverride(BaseModel):
    minio_bucket: Optional[str] = Field(
        default=None, description="Override target object store bucket name"
    )
    clickhouse_load: bool = Field(
        default=False, description="Coordinate direct downsink analytical injection"
    )


class ScanFilters(BaseModel):
    last_modified_after: Optional[str] = Field(
        default=None,
        description="ISO8601 Timestamp delimiter targeting delta-incremental syncing loops",
    )


class SalesforceSyncRequest(BaseModel):
    scan_id: str = Field(
        ...,
        description="Unique transaction tracker string emitted by Core Orchestrator",
    )
    org_id: str = Field(
        ..., description="Target corporate entity cluster organization identifier"
    )
    objects: List[str] = Field(
        ..., description="Collection of Salesforce CRM target modules to pull down"
    )
    filters: Optional[ScanFilters] = Field(default_factory=ScanFilters)
    output_format: str = Field(
        default="parquet", description="Target binary file compaction layout format"
    )
    destination: Optional[DestinationOverride] = Field(
        default_factory=DestinationOverride
    )


class SalesforceSyncResponse(BaseModel):
    success: bool
    scan_id: str
    status: str
    message: str


# ==========================================================================
# 🚀 Asynchronous Background Worker Task
# ==========================================================================
async def execute_background_sync(
    scan_id: str, org_id: str, objects: List[str], filters_dict: Dict[str, Any]
):
    try:
        await extraction_service.run_bulk_extraction(
            scan_id=scan_id, org_id=org_id, objects=objects, filters=filters_dict
        )
    except Exception as e:
        logger.critical(
            f"Fatal crash inside background extraction task worker thread for Scan {scan_id}: {e}"
        )


# ==========================================================================
# 🔌 Webhook API Route Handler
# ==========================================================================
@router.post(
    "/salesforce",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=SalesforceSyncResponse,
    summary="Trigger Salesforce Account Sync",
)
async def trigger_salesforce_sync(
    payload: SalesforceSyncRequest,
    background_tasks: BackgroundTasks,
    # This explicitly binds the security layer to the route for Swagger UI exposure
    authenticated_as: str = Depends(verify_hmac),
):
    """
    Webhook endpoint to trigger the Salesforce-to-MinIO ETL pipeline.
    Protected by upstream HMAC SHA-256 signature middleware validation.
    """
    logger.info(
        f"📥 Received inbound sync activation request for Scan: {payload.scan_id} (Org: {payload.org_id})"
    )

    if not payload.objects:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Sync initialization failed: Extraction object list collection cannot be empty.",
        )

    filters_dict = (
        {"last_modified_after": payload.filters.last_modified_after}
        if payload.filters
        else {}
    )

    # Dispatch data sync routine natively directly inside FastAPI's async execution loop
    background_tasks.add_task(
        execute_background_sync,
        scan_id=payload.scan_id,
        org_id=payload.org_id,
        objects=payload.objects,
        filters_dict=filters_dict,
    )

    return SalesforceSyncResponse(
        success=True,
        scan_id=payload.scan_id,
        status="started",
        message=f"Successfully initialized background Bulk extraction for {len(payload.objects)} objects. Extraction processing asynchronously.",
    )


# ==========================================================================
# 🩺 Nomad Deployment Health Probe
# ==========================================================================
@router.get(
    "/health",
    status_code=status.HTTP_200_OK,
    summary="Nomad Deployment Health Probe",
)
async def health_check(response: Response):
    """
    Unauthenticated health monitoring check required by the Nomad cluster orchestrator.
    Evaluates end-to-end downstream connectivity status for core infrastructure dependencies.
    """
    health_status = {
        "status": "healthy",
        "service": "black-diamond-salesforce-service",
        "version": "1.0.0",
        "salesforce_connected": False,
        "minio_connected": False,
    }

    is_degraded = False

    # 1. Evaluate Salesforce Client Core State
    try:
        if extraction_service.bulk_client:
            health_status["salesforce_connected"] = True
    except Exception as e:
        logger.warning(f"Health Probe failure evaluating Salesforce client state: {e}")
        is_degraded = True

    # 2. Evaluate MinIO Cluster Bucket Existence
    try:
        target_bucket = getattr(
            extraction_service.minio_client, "bucket_name", "glynac-raw-zone"
        )
        if extraction_service.minio_client.client.bucket_exists(target_bucket):
            health_status["minio_connected"] = True
    except Exception as e:
        logger.warning(
            f"Health Probe failure verifying MinIO object store bucket access: {e}"
        )
        is_degraded = True

    # Handle system-wide state transitions if dependencies are offline
    if is_degraded:
        health_status["status"] = "degraded"
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        logger.error(
            f"🚨 System infrastructure connection failure. Status DEGRADED: {health_status}"
        )
    else:
        response.status_code = status.HTTP_200_OK

    return health_status


# ==========================================================================
# 🧹 Administrative Housecleaning Endpoint
# ==========================================================================
@router.post(
    "/maintenance/cleanup",
    status_code=status.HTTP_200_OK,
    summary="Purge Old Scan Records",
)
async def trigger_system_cleanup(
    # Binds administrative routes strictly to HMAC verification (e.g., engineer keys)
    authenticated_as: str = Depends(verify_hmac),
):
    """
    Administrative housecleaning gateway endpoint.
    Purges log track tracking records older than the configured CLEANUP_DAYS threshold.
    """
    logger.info("⚠️ Administrative cleanup request received via secure gateway.")

    cleanup_summary = await maintenance_service.purge_stale_records()
    return cleanup_summary
