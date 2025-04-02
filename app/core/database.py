import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env if it exists

# Use environment variable or default to sqlite file in project root
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./movie_app.db") # Changed default DB name

# check_same_thread is needed only for SQLite. It's not needed for other databases.
engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()