# from sqlalchemy.orm import Session
# from . import models, schemas
# import pandas as pd
# from app.logging.logger import logging

# def save_movie_records(db: Session, task_id: int, records_df: pd.DataFrame):
#     """Saves processed movie records to the database."""
#     records_to_insert = []
#     for _, row in records_df.iterrows():
#         try:
#             # Clean the data
#             title = row["title"].strip() if pd.notna(row["title"]) else None
#             release_date = pd.to_datetime(row["release_date"], errors="coerce") if pd.notna(row["release_date"]) else None
#             genre = row["genre"].strip() if pd.notna(row["genre"]) else None
#             rating = float(row["rating"]) if pd.notna(row["rating"]) else None
#             overview = row["overview"].strip() if pd.notna(row["overview"]) else None
#             director = row["director"].strip() if pd.notna(row["director"]) else None
#             source = row["source"].strip() if pd.notna(row["source"]) else None

#             record_data = schemas.MovieRecordCreate(
#                 task_id=task_id,
#                 title=title,
#                 release_date=release_date,
#                 genre=genre,
#                 rating=rating,
#                 overview=overview,
#                 director=director,
#                 source=source,
#             )
#             records_to_insert.append(models.MovieRecord(**record_data.dict()))
#         except Exception as e:
#             logging.error(f"Error processing row: {row.to_dict()}. Error: {e}")

#     if records_to_insert:
#         db.bulk_save_objects(records_to_insert)
#         db.commit()
#         logging.info(f"Saved {len(records_to_insert)} records for task {task_id}")
