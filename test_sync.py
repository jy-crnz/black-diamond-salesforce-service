import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

# Import your FastAPI app instance and the security dependency
from app.main import app
from app.core.security import verify_hmac

# Create a synchronous test client
client = TestClient(app)


# ---------------------------------------------------------
# 🛡️ Bypass the HMAC security gateway for routing tests
# ---------------------------------------------------------
def override_verify_hmac():
    return "authorized_test_user"


app.dependency_overrides[verify_hmac] = override_verify_hmac


# ==========================================================================
# 🚀 Salesforce Extraction Route Tests
# ==========================================================================


def test_sync_rejects_empty_objects():
    """Ensure the route catches empty object arrays before triggering background workers."""
    payload = {
        "scan_id": "test-scan-001",
        "org_id": "glynac-org-001",
        "objects": [],
        "filters": {"last_modified_after": "2026-01-01T00:00:00Z"},
    }
    response = client.post("/v1/sync/salesforce", json=payload)
    assert response.status_code == 400
    assert "cannot be empty" in response.json()["detail"]


def test_sync_rejects_missing_scan_id():
    """Ensure Pydantic strictly validates missing required fields without crashing."""
    payload = {"org_id": "glynac-org-001", "objects": ["Contact"]}
    response = client.post("/v1/sync/salesforce", json=payload)
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "scan_id"]


@patch("app.api.routes.sync.execute_background_sync")
def test_sync_accepts_valid_payload(mock_execute):
    """Ensure a valid payload returns 202 Accepted and formats the response correctly."""
    payload = {
        "scan_id": "test-scan-happy",
        "org_id": "glynac-org-001",
        "objects": ["Contact", "Account"],
    }
    response = client.post("/v1/sync/salesforce", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["success"] is True
    assert data["status"] == "started"
    assert mock_execute.called


# ==========================================================================
# 🩺 Health Probe Endpoint Tests
# ==========================================================================


@patch("app.api.routes.sync.extraction_service")
def test_health_check_happy_path(mock_extraction_service):
    """Ensure the health endpoint returns 200 OK when infrastructure modules are active."""
    # Since MagicMocks are dynamically generated and inherently truthy,
    # the boolean connectivity checks will pass naturally.
    # We explicitly force the MinIO bucket check to evaluate to True:
    mock_extraction_service.minio_client.client.bucket_exists.return_value = True

    # Trigger our health checker
    response = client.get("/v1/sync/health")
    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "healthy"
    assert data["salesforce_connected"] is True
    assert data["minio_connected"] is True
    assert data["kafka_connected"] is True


# ==========================================================================
# 🧹 Maintenance Cleanup Endpoint Tests
# ==========================================================================


@patch("app.api.routes.sync.maintenance_service.purge_stale_records")
def test_maintenance_cleanup_dispatches_successfully(mock_purge):
    """Ensure the maintenance route triggers the purge engine and passes back optimization stats."""
    mock_purge.return_value = {
        "success": True,
        "purged_before": "2026-05-10T00:00:00Z",
        "retention_window_days": 7,
        "status": "system_optimized",
    }

    response = client.post("/v1/sync/maintenance/cleanup")
    assert response.status_code == 200

    data = response.json()
    assert data["success"] is True
    assert data["status"] == "system_optimized"
    assert mock_purge.called
