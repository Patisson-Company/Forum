from db.base import Base
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, String, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ulid import ULID


def ulid() -> str:
    return str(ULID())

class Message(Base):
    __tablename__ = "messages"

    id = Column(String, primary_key=True, default=ulid)
    user_id = Column(String)
    content = Column(Text)
    timestamp = Column(DateTime, default=func.now())
    topic_id = Column(String, ForeignKey('topics.id'))
    is_delete = Column(Boolean, default=False)
    
    topic = relationship("Topic", back_populates="messages")


class Topic(Base):
    __tablename__ = 'topics'

    id = Column(String, primary_key=True)
    messages = relationship("Message", back_populates="topic")