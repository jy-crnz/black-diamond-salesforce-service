import uuid
from datetime import datetime, timezone
from typing import Optional
from sqlmodel import SQLModel, Field


class DBBaseModel(SQLModel):
    """Base model enforcing strict structural standards across all tables."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    )


class SalesforceAccount(DBBaseModel, table=True):
    __tablename__ = "salesforce_account"

    # We store the Salesforce 18-char ID separately from our local UUID primary key
    salesforce_id: str = Field(unique=True, index=True, max_length=18)
    name: str
    type: Optional[str] = None
    industry: Optional[str] = None
