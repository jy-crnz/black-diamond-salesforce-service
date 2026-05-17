import logging
from typing import AsyncGenerator
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings

logger = logging.getLogger(__name__)

# Fallback to local if DATABASE_URL isn't set (fail-safe)
DATABASE_URL = getattr(
    settings,
    "DATABASE_URL",
    "postgresql+asyncpg://dev_user:dev_pass@localhost:5432/salesforce_dev",
)

# 1. Create the Async Engine
# pool_pre_ping=True ensures the connection is still alive before sending a query
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    pool_pre_ping=True,
    connect_args={"prepared_statement_cache_size": 0, "statement_cache_size": 0},
)

# 2. Create the Async Session Maker
async_session_maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# 3. Create the FastAPI Dependency
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency to be injected into FastAPI routes.
    Ensures that database sessions are securely opened and closed per request.
    """
    async with async_session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception as e:
            logger.error(f"Database session error, rolling back: {e}")
            await session.rollback()
            raise e
        finally:
            await session.close()
