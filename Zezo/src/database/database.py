import uuid
from datetime import datetime

from sqlalchemy import MetaData, create_engine, func
from sqlalchemy.sql.elements import literal_column

from src.utils.config import get_database_url

# Create sqlalchemy database engine
engine = create_engine(get_database_url())

# create metadata bind from engine
metaData = MetaData()
metaData.bind = engine

# use the uuid generate version
new_uuid = uuid.uuid4

# defined utc time
now = datetime.utcnow

# defined default time
default_now = dict(default=now, server_default=func.now())

# defined literal_column so we can use it to return all column from the schema
ALL_COLUMNS = literal_column("*")
