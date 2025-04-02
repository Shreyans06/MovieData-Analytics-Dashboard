# app/core/data_processor.py
import pandas as pd
import requests
import os
from datetime import datetime
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv
from urllib.parse import urljoin  # To construct URLs safely
from app.logging.logger import logging
import json

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_BASE_URL = "https://api.themoviedb.org/3/"
SOURCE_A_MOVIE_PATH = "data/tmdb_5000_movies.csv"  # Path to your movie CSV

# --- TMDb Helper ---
_tmdb_genre_map = None  # Cache for genre IDs to names


def get_tmdb_genre_map():
    """Fetches and caches the TMDb genre ID to name mapping."""
    global _tmdb_genre_map
    if _tmdb_genre_map is None:
        if not TMDB_API_KEY:
            logging.error("TMDB_API_KEY not configured.")
            return {}
        try:
            url = urljoin(TMDB_BASE_URL, "genre/movie/list")
            params = {"api_key": TMDB_API_KEY}
            response = requests.get(url, params=params)
            response.raise_for_status()
            genres = response.json().get("genres", [])
            _tmdb_genre_map = {genre["id"]: genre["name"] for genre in genres}
            logging.info(f"Fetched TMDb genre map: {_tmdb_genre_map}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch TMDb genre map: {e}")
            return {}  # Return empty map on error
    return _tmdb_genre_map


def map_genre_ids_to_names(genre_ids: List[int]) -> str:
    """Converts a list of TMDb genre IDs to a comma-separated string of names."""
    if not genre_ids:
        return ""
    genre_map = get_tmdb_genre_map()
    names = [genre_map.get(gid, str(gid)) for gid in genre_ids]  # Use ID if name not found
    return ",".join(names)


# --- Data Loading Functions ---


def fetch_tmdb_movies(filters: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """Fetches movie data from TMDb Discover endpoint based on filters."""
    if not TMDB_API_KEY:
        logging.error("Cannot fetch from TMDb: API key not set.")
        return None

    logging.info(f"Fetching TMDb movies with filters: {filters}")
    movies_data = []
    page = 1
    max_pages = 5  # Limit pages to avoid excessive calls in demo

    # Prepare base params for TMDb API
    api_params = {
        "api_key": TMDB_API_KEY,
        "sort_by": "popularity.desc",  # Or 'release_date.desc', 'vote_average.desc'
        "include_adult": "false",
        "language": "en-US",
    }

    # Add filters to API params
    if filters.get("start_year"):
        api_params["primary_release_date.gte"] = f"{filters['start_year']}-01-01"
    if filters.get("end_year"):
        api_params["primary_release_date.lte"] = f"{filters['end_year']}-12-31"
    if filters.get("min_rating_tmdb"):
        api_params["vote_average.gte"] = filters["min_rating_tmdb"]
    # TMDb API often uses genre IDs. Filtering by name requires extra logic or filtering post-fetch.
    # Let's filter post-fetch for simplicity here. Keep track of genre names requested.
    required_genres_tmdb = set(g.lower() for g in filters.get("genres_tmdb", []) if g)

    try:
        while page <= max_pages:
            api_params["page"] = page
            url = urljoin(TMDB_BASE_URL, "discover/movie")
            response = requests.get(url, params=api_params)
            response.raise_for_status()  # Raise HTTPError for bad responses (4XX, 5XX)
            data = response.json()
            results = data.get("results", [])
            if not results:
                break  # No more results

            movies_data.extend(results)

            if page >= data.get("total_pages", 1):
                break  # Reached the end
            page += 1

        logging.info(f"Fetched {len(movies_data)} raw movie entries from TMDb across {page-1} pages.")
        if not movies_data:
            return pd.DataFrame()  # Return empty DataFrame

        # Process results into a DataFrame
        processed_list = []
        for movie in movies_data:
            genre_names = map_genre_ids_to_names(movie.get("genre_ids", []))
            # Apply genre filter if specified
            if required_genres_tmdb:
                movie_genres_lower = set(g.lower() for g in genre_names.split(",") if g)
                if not required_genres_tmdb.issubset(
                    movie_genres_lower
                ):  # Check if all required genres are present
                    continue  # Skip movie if genres don't match

            processed_list.append(
                {
                    "title": movie.get("title"),
                    # Use original_title if title is missing? Your choice.
                    # 'original_title': movie.get('original_title'),
                    "release_date": pd.to_datetime(movie.get("release_date"), errors="coerce"),
                    "genre": genre_names,
                    "rating": movie.get("vote_average"),
                    "overview": movie.get("overview"),
                    "director": None,  # TMDb discover doesn't usually return director directly, needs extra API call per movie
                    "source": "TMDb API",
                }
            )

        df = pd.DataFrame(processed_list)
        df = df.dropna(subset=["title", "release_date"])  # Drop movies with critical missing info
        logging.info(f"Processed {len(df)} valid movies from TMDb after filtering.")
        return df

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from TMDb: {e}", exc_info=True)
        return None
    except Exception as e:
        logging.error(f"Error processing TMDb data: {e}", exc_info=True)
        return None


def load_and_filter_movie_csv(file_path: str, filters: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """Loads, filters, and standardizes movie data from the local CSV file."""
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded {len(df)} records from {file_path}")

        # Standardize columns (adjust based on your actual CSV headers)
        # rename_map = {
        #     "Budget" : "budget",
        #     "GenreList": "genres",
        #     "Movie_ID" : "id",
        #     "Language" : "original_language",
        #     "MovieTitle": "original_title",
        #     "ReleaseDate": "release_date",
        #     "Revenue" : "revenue",
        #     "Runtime" : "runtime",
        #     "Rating": "vote_average",
        #     # Add others if needed
        # }
        # actual_rename = {k: v for k, v in rename_map.items() if v in df.columns}
        # df = df.rename(columns=actual_rename)

        # Convert release_date string to datetime objects
        if "release_date" in df.columns:
            df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
        else:
            logging.warning("CSV: No 'ReleaseDate' or 'release_date' column found.")
            # Handle as needed - maybe return None or empty DF
        print(df.columns)
        
        df = df.dropna(subset=["original_title", "release_date" , "runtime"])  # Require title and valid date

        # Ensure rating is numeric and scale if needed (e.g., if CSV is /5, multiply by 2)
        if "vote_average" in df.columns:
            df["vote_average"] = pd.to_numeric(df["vote_average"], errors="coerce")
            # Example Scaling: If CSV uses /5 scale, uncomment below
            # df['rating'] = df['rating'] * 2
            df = df.dropna(subset=["vote_average"])  # Optionally drop rows with invalid ratings
        else:
            logging.warning("CSV: No'vote_average' column found.")

        # --- Apply Filters ---
        original_count = len(df)
        if filters.get("start_year"):
            df = df[df["release_date"].dt.year >= int(filters["start_year"])]
        if filters.get("end_year"):
            df = df[df["release_date"].dt.year <= int(filters["end_year"])]
        # if filters.get("genre"):
            # df = df[df["genres"].apply(lambda x: filters["genre"] in [v["name"] for v in json.loads(x)])]
        if filters.get("min_rating"):
            df = df[df["vote_average"] >= float(filters["min_rating"])]
        if filters.get("language"):
            df = df[df["original_language"] == filters["language"]]

        logging.info(f"CSV: Filtered from {original_count} to {len(df)} records.")

        # Select final columns, ensure all exist
        final_cols = ["budget", "genres" , "id" , "original_language", "original_title", "release_date", "revenue", "runtime", "vote_average", "vote_count"]    

        return df[final_cols]  # Return with consistent column order

    except FileNotFoundError:
        logging.error(f"Movie CSV file not found at {file_path}")
        return None
    except Exception as e:
        logging.error(f"Error processing Movie CSV {file_path}: {e}", exc_info=True)
        return None


def fetch_and_process_data(filters: Dict[str, Any]) -> pd.DataFrame:
    """Fetches movie data from TMDb and local CSV based on filters, merges, and returns a unified DataFrame."""
    logging.info(f"Starting movie data fetch and processing with filters: {filters}")

    # df_tmdb = fetch_tmdb_movies(filters)
    df_csv = load_and_filter_movie_csv(SOURCE_A_MOVIE_PATH, filters)

    valid_dfs = [df for df in [df_csv] if df is not None and not df.empty]

    expected_columns = ["title", "release_date", "genre", "rating", "overview", "director", "source"]
    if not valid_dfs:
        logging.warning("No movie data retrieved from any source after filtering.")
        return pd.DataFrame(columns=expected_columns)

    # Ensure columns match before concat
    for i, df in enumerate(valid_dfs):
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None  # Add missing columns
        valid_dfs[i] = df[expected_columns]  # Enforce column order

    try:
        unified_df = pd.concat(valid_dfs, ignore_index=True, sort=False)
    except Exception as e:
        logging.error(f"Error during DataFrame concatenation: {e}", exc_info=True)
        return pd.DataFrame(columns=expected_columns)

    # --- Deduplication (Optional but Recommended) ---
    # Simple approach: keep first occurrence based on title and year
    if "release_date" in unified_df.columns and not unified_df.empty:
        unified_df["year"] = unified_df["release_date"].dt.year
        original_count = len(unified_df)
        # Convert title to lowercase for case-insensitive matching
        unified_df["title_lower"] = unified_df["title"].str.lower().str.strip()
        unified_df = unified_df.sort_values(by=["source"], ascending=True)  # Prioritize one source? e.g. TMDb
        unified_df = unified_df.drop_duplicates(subset=["title_lower", "year"], keep="first")
        unified_df = unified_df.drop(columns=["title_lower", "year"])  # Remove helper columns
        deduplicated_count = original_count - len(unified_df)
        if deduplicated_count > 0:
            logging.info(f"Removed {deduplicated_count} duplicate movie records based on title and year.")
    else:
        # Basic title deduplication if year is unavailable
        original_count = len(unified_df)
        unified_df["title_lower"] = unified_df["title"].str.lower().str.strip()
        unified_df = unified_df.drop_duplicates(subset=["title_lower"], keep="first")
        unified_df = unified_df.drop(columns=["title_lower"])
        deduplicated_count = original_count - len(unified_df)
        if deduplicated_count > 0:
            logging.info(f"Removed {deduplicated_count} duplicate movie records based on title.")

    # Final sorting
    if "release_date" in unified_df.columns:
        unified_df = unified_df.sort_values(by="release_date", ascending=False)

    logging.info(f"Unified movie data contains {len(unified_df)} records after processing and deduplication.")
    return unified_df
