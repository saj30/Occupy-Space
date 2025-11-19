#!/usr/bin/env python3
"""
Occupy Space – NASA API Data Viewer (stub template)

This script:
1. Loads the NASA API key from config/nasa_api_key.txt
2. Fetches APOD + NeoWs data
3. Saves results to SQLite (database.db)
4. Generates index.html via simple string substitution
"""

import json
import sqlite3
import urllib.request
from datetime import date
from pathlib import Path


# ---------- CONFIG ----------
DB_FILE      = "database.db"
API_KEY_FILE = "config/nasa_api_key.txt"
TEMPLATE_HTML= "templates/index.html"
OUTPUT_HTML  = "index.html"


# ---------- API KEY ----------
def load_api_key() -> str:
    """
    Read and return the NASA API key stored in config/nasa_api_key.txt
    
    Returns
    -------
    str
        The 40-character NASA API key with leading/trailing whitespace removed.
        Raises FileNotFoundError if the file does not exist.
    """
    pass


# ---------- NASA FETCHERS ----------
def fetch_apod(api_key: str) -> dict:
    """
    Query https://api.nasa.gov/planetary/apod
    
    Parameters
    ----------
    api_key : str
        Valid NASA API key (40-char string).
    
    Returns
    -------
    dict
        JSON response converted to Python dict containing at minimum:
        - 'title'        : picture title
        - 'date'         : ISO date string (YYYY-MM-DD)
        - 'explanation'  : long description text
        - 'url'          : direct link to HD image (or video thumbnail)
        
    Raises
    ------
    urllib.error.HTTPError
        On non-200 status from NASA.
    json.JSONDecodeError
        If response body is not valid JSON.
    """
    pass


def fetch_neo(api_key: str) -> dict:
    """
    Query https://api.nasa.gov/neo/rest/v1/feed for *today only*.
    
    Parameters
    ----------
    api_key : str
        Valid NASA API key.
    
    Returns
    -------
    dict
        Full JSON from NeoWs feed for today. Top-level structure:
        {
          "near_earth_objects": {
             "YYYY-MM-DD": [ ...list of NEO dicts... ]
          }
        }
        
    Raises
    ------
    urllib.error.HTTPError
        On non-200 status.
    json.JSONDecodeError
        If response body is not valid JSON.
    """
    pass


# ---------- DATABASE ----------
def get_db_connection():
    """
    Create (if necessary) and return an sqlite3 connection object
    with row_factory set to sqlite3.Row for dict-like access.
    
    Returns
    -------
    sqlite3.Connection
        Active connection to database.db; caller must close when finished.
    """
    pass


def init_db():
    """
    Ensure both required tables exist with exact schema from spec:
    
    CREATE TABLE IF NOT EXISTS apod (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        date TEXT,
        explanation TEXT,
        image_url TEXT
    );
    
    CREATE TABLE IF NOT EXISTS neo (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        count INTEGER,
        smallest REAL,
        largest REAL
    );
    
    Commits immediately so tables persist.
    """
    pass


def save_apod_to_db(data: dict):
    """
    Insert a single APOD record into the `apod` table.
    
    Parameters
    ----------
    data : dict
        Must contain keys: 'title', 'date', 'explanation', 'url'.
        Values are inserted verbatim; no additional escaping is performed
        (sqlite3 module handles parameter substitution).
    
    Side-effects
    -------------
    Opens DB connection, inserts row, commits, closes connection.
    """
    pass


def save_neo_to_db(neo_date: str, count: int, smallest: float, largest: float):
    """
    Insert a single NEO daily summary into the `neo` table.
    
    Parameters
    ----------
    neo_date : str
        ISO date string (YYYY-MM-DD) for this summary.
    count : int
        Total number of NEOs listed for that date.
    smallest : float
        Smallest estimated diameter (km) among all NEOs that day.
    largest : float
        Largest estimated diameter (km) among all NEOs that day.
    
    Side-effects
    -------------
    Opens DB connection, inserts row, commits, closes connection.
    """
    pass


def latest_apod() -> dict | None:
    """
    Fetch the most recently inserted APOD row.
    
    Returns
    -------
    dict | None
        Row converted to plain dict via dict(sqlite3.Row). Keys:
        'id', 'title', 'date', 'explanation', 'image_url'.
        Returns None if the `apod` table is empty.
    """
    pass


def latest_neo() -> dict | None:
    """
    Fetch the most recently inserted NEO summary row.
    
    Returns
    -------
    dict | None
        Row as dict with keys:
        'id', 'date', 'count', 'smallest', 'largest'.
        Returns None if the `neo` table is empty.
    """
    pass


# ---------- HTML ----------
def render_html(apod_row: dict, neo_row: dict) -> str:
    """
    Read templates/index.html, substitute {{placeholders}} with actual data,
    and return the final HTML string ready to be written to disk.
    
    Parameters
    ----------
    apod_row : dict
        Latest APOD record; must contain at least:
        'title', 'date', 'explanation', 'image_url'.
    neo_row : dict
        Latest NEO summary; must contain at least:
        'count', 'smallest', 'largest'.
    
    Returns
    -------
    str
        Fully rendered HTML with all placeholders replaced.
    """
    pass


# ---------- MAIN ORCHESTRATOR ----------
def process():
    """
    High-level pipeline:
    1. init_db() – ensure tables exist
    2. api_key = load_api_key()
    3. apod_json = fetch_apod(api_key)
       save_apod_to_db(apod_json)
    4. neo_json  = fetch_neo(api_key)
       count, smallest, largest = process neo_json for today's date
       save_neo_to_db(today_str, count, smallest, largest)
    5. apod_row = latest_apod()
       neo_row  = latest_neo()
    6. html = render_html(apod_row, neo_row)
       write html to OUTPUT_HTML (index.html)
    7. Print success message to console.
    
    Side-effects
    -------------
    Creates or updates database.db and index.html in the project root.
    """
    pass


# ---------- ENTRY ----------
if __name__ == "__main__":
    process()