from sqlalchemy import Column, DateTime, String, Table, Integer
from sqlalchemy.dialects.postgresql import UUID

from src.database.database import default_now, metaData, new_uuid, now

tracking = Table(
    "tracking",
    metaData,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        nullable=False,
        default=new_uuid,
        index=True,
    ),
    Column("image", String, nullable=False),
    Column("image_input", String, nullable=False),
    Column("image_output", String, nullable=False),
    Column("credits", Integer, nullable=False),
    Column("response_time", DateTime, nullable=False, onupdate=now, **default_now),
    Column("created_at", DateTime, nullable=False, onupdate=now, **default_now),
    Column("updated_at", DateTime, nullable=False, onupdate=now, **default_now),
)
