from sqlalchemy import (
    Column,
    Integer
)

from sqlalchemy.ext.declarative import declarative_base

__all__ = [
    'Base'
]

Base = declarative_base()

class Interactions(Base):
    __tablename__ = 'interactions'
    id = Column(Integer, primary_key = True)
