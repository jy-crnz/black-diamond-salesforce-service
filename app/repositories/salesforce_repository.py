import logging
from datetime import datetime, timezone
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.domain import SalesforceAccount

logger = logging.getLogger(__name__)


class SalesforceRepository:
    """
    Data Access Layer for Salesforce data.
    Isolates database interactions from business logic.
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def bulk_upsert_accounts(self, accounts_data: list[dict]) -> int:
        """
        Takes a list of dictionaries, maps them to the database, and performs
        an upsert. If the salesforce_id already exists, it updates the record.
        """
        if not accounts_data:
            return 0

        # Inject our updated_at timestamp into the incoming data
        current_time = datetime.now(timezone.utc).replace(tzinfo=None)
        for data in accounts_data:
            data["updated_at"] = current_time

        # 1. Create the base PostgreSQL INSERT statement
        stmt = insert(SalesforceAccount).values(accounts_data)

        # 2. Define the columns to update if there is a conflict (record exists)
        # We map the existing columns to the new incoming data (stmt.excluded)
        update_dict = {
            "name": stmt.excluded.name,
            "type": stmt.excluded.type,
            "industry": stmt.excluded.industry,
            "updated_at": stmt.excluded.updated_at,
        }

        # 3. Add the ON CONFLICT DO UPDATE clause
        # This tells Postgres: "If salesforce_id already exists, just update these fields"
        stmt = stmt.on_conflict_do_update(
            index_elements=["salesforce_id"], set_=update_dict
        )

        # 4. Execute the statement
        try:
            await self.session.execute(stmt)
            # We don't commit here. The dependency injection in database.py handles commits!
            logger.info(f"✅ Upserted batch of {len(accounts_data)} accounts.")
            return len(accounts_data)
        except Exception as e:
            logger.error(f"❌ Failed to bulk upsert accounts: {str(e)}")
            raise e
