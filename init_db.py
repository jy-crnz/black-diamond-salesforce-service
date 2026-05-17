import asyncio
import logging
from sqlmodel import SQLModel
from sqlalchemy.ext.asyncio import create_async_engine
from app.core.config import settings
from app.models.domain import SalesforceAccount

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Reformat the URL for asyncpg
DATABASE_URL = getattr(settings, "DATABASE_URL")


async def init_db():
    logger.info("Connecting to Supabase to create tables...")
    engine = create_async_engine(DATABASE_URL, echo=True)

    async with engine.begin() as conn:
        # This reads our SQLModel classes and generates the actual Postgres tables
        await conn.run_sync(SQLModel.metadata.create_all)

    logger.info("✅ Database initialization complete! Tables created successfully.")


if __name__ == "__main__":
    asyncio.run(init_db())
