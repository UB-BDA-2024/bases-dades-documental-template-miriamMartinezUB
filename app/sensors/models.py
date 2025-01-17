import datetime

from sqlalchemy import Column, DateTime, Integer, String

from ..database import Base


class Sensor(Base):
    __tablename__ = "sensors"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    joined_at = Column(DateTime, default=datetime.datetime.utcnow)
