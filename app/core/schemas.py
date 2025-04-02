from optparse import Option
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any , Tuple
from datetime import datetime, date # Added date
import enum
class TaskStatus(str, enum.Enum):
    PENDING = "pending"
    IN_PROGRESS = "in progress"
    COMPLETED = "completed"
    FAILED = "failed"

# --- MovieRecord Schemas ---
class MovieRecordBase(BaseModel):
    original_title: Optional[str]
    release_date: Optional[datetime] = None 
    genres: Optional[str] = None 
    vote_average: Optional[float] = None 
    runtime: Optional[int] = None
    revenue: Optional[int] = None
    budget: Optional[int] = None
    vote_count: Optional[int] = None
    original_language: Optional[str] = None

class MovieRecordCreate(MovieRecordBase):
    task_id: int

    class Config:
        from_attributes = True # Pydantic V1. Use from_attributes=True for V2
    
class MovieRecordRead(MovieRecordBase):
    task_id: int

    class Config:
        from_attributes = True # Pydantic V1. Use from_attributes=True for V2

# --- Task Schemas ---
class TaskFilterParams(BaseModel):
    # Filters relevant to movies
    start_year: Optional[int] = Field(None, description="Filter movies released from this year onwards (inclusive).")
    end_year: Optional[int] = Field(None, description="Filter movies released up to this year (inclusive).")
    genres_csv: Optional[List[str]] = Field(None, description="List of genres (case-insensitive contains) to include from CSV.")
    min_rating: Optional[float] = Field(None, ge=0, le=10, description="Minimum user score (0-10) from CSV.")
    language: Optional[str] = Field(None , description = "Filter movies in this language")
    # Add more filters if needed: director, keywords, etc.

class TaskCreate(BaseModel):
    filters: TaskFilterParams

class TaskRead(BaseModel):
    id: int
    status: TaskStatus
    created_at: datetime
    updated_at: Optional[datetime] = None
    filters: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True # Pydantic V1