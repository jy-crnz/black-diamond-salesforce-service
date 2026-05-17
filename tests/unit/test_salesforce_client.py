import pytest
import respx
from app.auth.salesforce_auth import token_manager
from app.clients.bulk_api_client import SalesforceBulkAPIClient

# Tell pytest this file uses async code
pytestmark = pytest.mark.asyncio


@respx.mock
async def test_salesforce_token_and_job_creation():
    """
    Proves that our Token Manager and Bulk API Client can successfully
    authenticate and create a job when Salesforce responds correctly.
    """
    # 1. Mock the OAuth Token endpoint
    respx.post("https://login.salesforce.com/services/oauth2/token").respond(
        status_code=200,
        json={
            "access_token": "mocked_token_12345",
            "instance_url": "https://mock.salesforce.com",
            "token_type": "Bearer",
        },
    )

    # 2. Mock the Bulk API Job Creation endpoint
    respx.post("https://mock.salesforce.com/services/data/v59.0/jobs/query").respond(
        status_code=200, json={"id": "7500W000000FAKE", "state": "UploadComplete"}
    )

    # --- Execute our actual code ---

    # Force the token manager to fetch our mocked token
    await token_manager._refresh_token()
    token, url = await token_manager.get_token()

    assert token == "mocked_token_12345"
    assert url == "https://mock.salesforce.com"

    # Test the job creation logic
    client = SalesforceBulkAPIClient()
    job_id = await client.create_query_job("SELECT Id, Name FROM Contact")

    # --- Verify the Results ---
    assert job_id == "7500W000000FAKE"
    print("\n✅ SUCCESS: Token retrieved and Bulk API Job created successfully!")
