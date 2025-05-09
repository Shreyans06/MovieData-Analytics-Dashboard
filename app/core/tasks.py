# from fastapi import APIRouter, Depends, HTTPException, Query, Body
# from sqlalchemy.orm import Session
# from typing import List, Optional, Dict, Any
# from datetime import datetime, date # Added date
# from core import models, schemas, queue_manager
# from core.database import get_db
# from sqlalchemy.sql import extract # For year extraction
# from core.logging.logger import logging

# router = APIRouter()

# @router.post(
#     "/tasks",
#     response_model=schemas.TaskRead, # Response is still TaskRead
#     status_code=201,
#     summary="Create Movie Data Processing Task",
#     description="Submit a new task to fetch and process movie data from TMDb and a local CSV based on the provided filters.",
# )
# def create_task(
#     task_in: schemas.TaskCreate = Body( # Input uses the updated TaskCreate schema
#         ...,
#         examples={
#             "year_range": {
#                 "summary": "Filter by Year",
#                 "value": {"filters": {"start_year": 2010, "end_year": 2020}},
#             },
#              "genre_and_rating": {
#                 "summary": "Filter by Genre and Rating",
#                 "value": {
#                     "filters": {
#                         "genres_tmdb": ["Action", "Adventure"], # Match TMDb genre names
#                         "genres_csv": ["Sci-Fi"], # Check contains in CSV genre string
#                         "min_rating_tmdb": 7.0,
#                         "min_rating_csv": 6.5
#                         }
#                     },
#             },
#         },
#     ),
#     db: Session = Depends(get_db)
# ) -> models.Task:
#     """
#     Creates a new movie data processing task:

#     - **task_in**: Input data containing filtering parameters for movies.
#         - Refer to `TaskFilterParams` schema for details (start/end year, genres, ratings).

#     - Stores task definition, adds to queue, returns created task object.
#     """
#     filters_dict = task_in.filters.dict(exclude_unset=True)

#     start_year = filters_dict.get('start_year')
#     end_year = filters_dict.get('end_year')
#     if start_year and end_year and start_year > end_year:
#          raise HTTPException(status_code=422, detail="Start year cannot be after end year.")

#     # Add validation for rating ranges if needed (Pydantic schema already does some)
#     min_r_tmdb = filters_dict.get('min_rating_tmdb')
#     min_r_csv = filters_dict.get('min_rating_csv')
#     if min_r_tmdb and not (0 <= min_r_tmdb <= 10):
#         raise HTTPException(status_code=422, detail="TMDb minimum rating must be between 0 and 10.")
#     if min_r_csv and not (0 <= min_r_csv <= 10):
#         raise HTTPException(status_code=422, detail="CSV minimum rating must be between 0 and 10.")


#     db_task = models.Task(status=models.TaskStatus.PENDING, filters=filters_dict)
#     db.add(db_task)
#     db.commit()
#     db.refresh(db_task)

#     logging.info(f"Created movie task {db_task.id} with filters: {filters_dict}")
#     queue_manager.add_task_to_queue(task_id=db_task.id, filters=filters_dict)
#     return db_task

# # list_tasks and get_task_status endpoints remain unchanged in logic,
# # but their responses (`TaskRead`) implicitly include the movie filters if present.
# # Add docstrings if desired.
# @router.get("/tasks", response_model=List[schemas.TaskRead], summary="List All Tasks")
# def list_tasks(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)) -> List[models.Task]:
#     # ... (implementation unchanged) ...
#     tasks = db.query(models.Task).order_by(models.Task.created_at.desc()).offset(skip).limit(limit).all()
#     return tasks

# @router.get("/tasks/{task_id}", response_model=schemas.TaskRead, summary="Get Task Status")
# def get_task_status(task_id: int, db: Session = Depends(get_db)) -> models.Task:
#     # ... (implementation unchanged) ...
#      db_task = db.query(models.Task).filter(models.Task.id == task_id).first()
#      if db_task is None:
#          raise HTTPException(status_code=404, detail="Task not found")
#      return db_task


# @router.get(
#     "/tasks/{task_id}/data",
#     # *** Change Response Model ***
#     response_model=List[schemas.MovieRecordRead],
#     summary="Get Task Results (Movie Data)",
#     description="Retrieves the processed movie data associated with a specific completed task. Allows optional server-side filtering.",
#      responses={
#         404: {"description": "Task not found"},
#         400: {"description": "Task is not yet completed or failed"},
#     },
# )
# def get_task_data(
#     task_id: int,
#     # *** Update Query Parameters for Movies ***
#     year: Optional[int] = Query(None, description="Filter results to include only movies released in this year."),
#     genre: Optional[str] = Query(None, description="Filter results by genre (case-insensitive partial match within the genre string)."),
#     min_rating: Optional[float] = Query(None, ge=0, le=10, description="Filter results by minimum rating."),
#     director: Optional[str] = Query(None, description="Filter results by director name (case-insensitive partial match)."),
#     db: Session = Depends(get_db)
#     # *** Change Return Type Hint ***
# ) -> List[models.MovieRecord]:
#     """
#     Retrieves the processed movie data for a completed task.

#     - **task_id**: The ID of the task.
#     - **year** (Optional query param): Filter by release year.
#     - **genre** (Optional query param): Filter if genre string contains this value.
#     - **min_rating** (Optional query param): Filter by minimum rating.
#     - **director** (Optional query param): Filter by director name.
#     """
#     db_task = db.query(models.Task).filter(models.Task.id == task_id).first()
#     if db_task is None:
#         raise HTTPException(status_code=404, detail="Task not found")
#     if db_task.status != models.TaskStatus.COMPLETED:
#         raise HTTPException(status_code=400, detail=f"Task status is {db_task.status}. Data is only available for 'completed' tasks.")

#     # *** Query the Correct Model ***
#     query = db.query(models.MovieRecord).filter(models.MovieRecord.task_id == task_id)

#     # Apply server-side filters
#     if year:
#          # Use SQLAlchemy's extract function for year filtering on DateTime column
#         query = query.filter(extract('year', models.MovieRecord.release_date) == year)
#         logging.debug(f"Applied year filter: {year} for task {task_id}")

#     if genre:
#         # Case-insensitive contains search on the genre string
#         query = query.filter(models.MovieRecord.genre.ilike(f"%{genre}%"))
#         logging.debug(f"Applied genre filter: {genre} for task {task_id}")

#     if min_rating is not None:
#         # Ensure rating column exists and filter
#         query = query.filter(models.MovieRecord.rating >= min_rating)
#         logging.debug(f"Applied min_rating filter: {min_rating} for task {task_id}")

#     if director:
#         query = query.filter(models.MovieRecord.director.ilike(f"%{director}%"))
#         logging.debug(f"Applied director filter: {director} for task {task_id}")


#     # Order results (e.g., by release date descending)
#     movie_records = query.order_by(models.MovieRecord.release_date.desc()).all()
#     logging.info(f"Retrieved {len(movie_records)} movie records for task {task_id} with server-side filters applied.")
#     return movie_records
