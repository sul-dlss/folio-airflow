import logging

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
)

from sqlalchemy.orm import declarative_base

logger = logging.getLogger(__name__)

Model = declarative_base()


class DigitalBookplate(Model):  # type: ignore
    __tablename__ = "digital_bookplates"

    id = Column(Integer, primary_key=True)
    created = Column(DateTime, nullable=False)
    updated = Column(DateTime, nullable=False)
    druid = Column(String, unique=True, nullable=False)
    name = Column(String, unique=True, nullable=True)
    image_filename = Column(String, nullable=False)
    title = Column(String, nullable=False)