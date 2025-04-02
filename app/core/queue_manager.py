import asyncio
from queue import Queue as SyncQueue # Standard sync queue
from threading import Thread, Event
from typing import Dict, Any
from sqlalchemy.orm import Session
from . import models, schemas
from .database import SessionLocal
import time # For simulation
from app.logging.logger import logging
import enum
from app.core.data_processor import load_and_filter_movie_csv
import json

# Use a standard thread-safe queue for simplicity with background threads
task_queue = SyncQueue()
stop_event = Event() # To signal the worker thread to stop

class TaskStatus(str, enum.Enum):
    PENDING = "pending"
    IN_PROGRESS = "in progress"
    COMPLETED = "completed"
    FAILED = "failed"

def create_task(db: Session, filters: Dict[str, Any]):
    """Creates a new task in the database."""
    db_task = models.Task(status=models.TaskStatus.PENDING, filters=filters)
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    return db_task.status
    
def update_task_status(db: Session, task_id: int, status: models.TaskStatus, error_message: str = None):
    """Updates the status of a task in the database."""
    db_task = db.query(models.Task).filter(models.Task.id == task_id).first()
    if db_task:
        db_task.status = status
        # Optionally store error message if failed
        # if error_message: db_task.error = error_message # Add an 'error' field to model if needed
        db.commit()
        logging.info(f"Task {task_id} status updated to {status}")
    else:
        logging.error(f"Task {task_id} not found for status update.")

def save_movie_records(db: Session, task_id: int, records_df):
    """Saves processed data records to the database."""
    records_to_insert = []
    for _, row in records_df.iterrows():
        record_data = schemas.MovieRecordCreate(
            task_id = task_id,
            original_title = row["original_title"],
            release_date = row["release_date"], 
            genres = ",".join([item["name"] for item in json.loads(row["genres"])]), 
            vote_average = row["vote_average"],
            runtime =  row["runtime"] , 
            revenue = row["revenue"],
            budget = row["budget"],
            vote_count = row["vote_count"],
            original_language = row["original_language"]
        )
        records_to_insert.append(models.MovieRecord(**record_data.dict())) # For Pydantic V1

    if records_to_insert:
        db.bulk_save_objects(records_to_insert)
        db.commit()
        logging.info(f"Saved {len(records_to_insert)} records for task {task_id}")

def task_worker():
    """Worker function to process tasks from the queue."""
    logging.info("Task worker started.")
    while not stop_event.is_set():
        try:
            # Get task from queue, block for 1 second if empty, then check stop_event
            if task_queue.empty():
                continue
            task_info = task_queue.get(block=True, timeout=1)
            if task_info is None: # Allow graceful shutdown
                continue

            task_id = task_info["task_id"]
            filters = task_info["filters"]
            logging.info(f"Processing task {task_id} with filters: {filters}")

            # Need a new DB session per task/thread
            db = SessionLocal()
            try:

                # create_task(db, filters)
                # logging.info(f"Added task {task_id} with filters: {filters} to DB")

                # 1. Update status to "in progress"
                update_task_status(db, task_id, models.TaskStatus.IN_PROGRESS)

                # 2. Simulate initial delay
                time.sleep(5) # Simulate work

                # 3. Fetch and process data
                logging.info(f"Fetching data for task {task_id}...")
                
                # 2. Load and Filter CSV
                file_path = "app/data/tmdb_5000_movies.csv"  
                filtered_df = load_and_filter_movie_csv(file_path, filters)

                if filtered_df is None or filtered_df.empty:
                    logging.warning("No data found after filtering.")
                    
                # processed_data_df = fetch_and_process_data(task_id , filters) # Pass filters directly

                # 4. Simulate processing delay
                time.sleep(5) # Simulate DB insertion / more work

                # 5. Save data to DB
                save_movie_records(db, task_id, filtered_df)

                # 6. Update status to "completed"
                update_task_status(db, task_id, models.TaskStatus.COMPLETED)
                logging.info(f"Task {task_id} completed successfully.")

            except Exception as e:
                logging.error(f"Error processing task {task_id}: {e}", exc_info=True)
                # Update status to "failed"
                update_task_status(db, task_id, models.TaskStatus.FAILED, error_message=str(e))
            finally:
                db.close() # Ensure session is closed
                task_queue.task_done() # Signal task completion to the queue

        except Exception as e:
            # Catch potential issues with getting from queue or unexpected errors
             logging.error(f"Worker loop error: {e}", exc_info=True)
             time.sleep(1) # Avoid busy-looping on error

    logging.info("Task worker stopped.")


# --- Worker Thread Management ---
worker_thread = None

def start_worker():
    """Starts the background worker thread."""
    global worker_thread
    if worker_thread is None or not worker_thread.is_alive():
        stop_event.clear()
        worker_thread = Thread(target=task_worker, daemon=True) # Daemon allows main thread to exit
        worker_thread.start()
        logging.info("Task worker thread started.")

def stop_worker():
    """Signals the background worker thread to stop."""
    global worker_thread
    if worker_thread and worker_thread.is_alive():
        stop_event.set()
        task_queue.put(None) # Add sentinel value to unblock the worker if waiting
        worker_thread.join(timeout=5) # Wait for worker to finish
        logging.info("Task worker thread stopped.")
        worker_thread = None

def add_task_to_queue(task_id: int, filters: Dict[str, Any]):
    """Adds a new task to the processing queue."""
    task_info = {"task_id": task_id, "filters": filters , "status" : TaskStatus.PENDING  }
    task_queue.put(task_info)
    logging.info(f"Task {task_id} added to queue.")