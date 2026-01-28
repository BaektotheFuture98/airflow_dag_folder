from sqlalchemy import Integer, String, DateTime, Bool
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class monitor(Base) : 