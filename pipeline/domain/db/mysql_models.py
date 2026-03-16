from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Monitor(Base):
    __tablename__ = "monitor"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    created_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
