from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base
import enum

class TaskStatus(str, enum.Enum):
    PENDING = "pending"
    IN_PROGRESS = "in progress"
    COMPLETED = "completed"
    FAILED = "failed"

class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, index=True)
    status = Column(String, default=TaskStatus.PENDING, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    filters = Column(JSON)

    # Relationship to link MovieRecords back to this task
    movie_records = relationship("MovieRecord", back_populates="task")

class MovieRecord(Base):
    __tablename__ = "movie_records"

    id = Column(Integer, index=True , primary_key=True)
    task_id = Column(Integer, ForeignKey("tasks.id"), index=True) # Foreign Key

    original_title = Column(String)
    release_date = Column(DateTime(timezone=True))
    genres = Column(String)
    vote_average = Column(Float)
    runtime = Column(Integer)
    revenue = Column(Integer)
    budget = Column(Integer)
    vote_count = Column(Integer)
    original_language = Column(String)

    # Relationship back to the Task
    task = relationship("Task", back_populates="movie_records")