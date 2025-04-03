# Full-Stack Movie Data Sourcing and Visualization App

This application allows users to create tasks to fetch movie data from a local CSV file, filter it based on user-defined criteria, store the unified results in a database, and visualize analytics derived from the retrieved data.

## Features

*   **Movie Data Task Creation:** Define tasks via a web UI to fetch movie information, specifying filters for release year range, specific genres (for each source), and minimum ratings (for each source).
*   **Dual Data Source:**
    *   **Local CSV File:** Reads movie data from a provided CSV file on your system. This allows you to use your own curated lists or combine data from other sources.
*   **Asynchronous Processing:** Tasks are processed in the background using a simulated in-memory job queue.
*   **Data Unification & Storage:** Movie data from CSV file is standardized and stored in a unified relational schema (SQLite database).
*   **Task Status Tracking:** Monitor the progress of tasks (Pending, In Progress, Completed, Failed) directly in the web interface.
*   **Interactive Data Visualization:**
    *   **Movies Released Per Year:** A bar chart showing the number of movies released each year within the task's data.
    *   **Average Rating by Genre:** A bar chart displaying the average movie rating for each genre.
*   **Dynamic Filtering:** Refine the visualized data on the frontend using dropdown filters for release year and genre, allowing you to explore specific subsets of your movie data.

## Tech Stack

*   **Backend:** FastAPI (Python Web Framework)
*   **Database:** SQLite (via SQLAlchemy ORM)
*   **Data Processing:** Pandas
*   **Job Queue:** Python's built-in `queue` and `threading` modules (for simulated asynchronous processing)
*   **Frontend:** HTML, CSS, JavaScript
*   **Visualization:** D3.js v7
