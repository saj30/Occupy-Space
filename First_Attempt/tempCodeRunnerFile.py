#!/usr/bin/env python3
"""
Occupy Space – NASA API Data Viewer (stub template)

This script:
1. Loads the NASA API key from config/nasa_api_key.txt
2. Fetches APOD + NeoWs data
3. Saves results to SQLite (database.db)
4. Generates index.html via simple string substitution
"""

import html
import json
from os import replace
import sqlite3
from tempfile import template
import urllib.request
import urllib.parse
from datetime import date
from pathlib import Path
from typing import Optional
import ssl

from numpy import save


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
    path = Path(API_KEY_FILE)
    if not path.exists():
        raise FileNotFoundError(f"API key file not found: {API_KEY_FILE}")
    key = path.read_text(encoding="utf-8").strip()
    if not key:
        raise ValueError("API key file is empty.")
    return key


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
    url = "https://api.nasa.gov/planetary/apod"
    params = {"api_key": api_key}
    return _get_json(url, params)


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
    url = "https://api.nasa.gov/neo/rest/v1/feed"
    today_str = date.today().isoformat()
    params = {
        "api_key": api_key,
        "start_date": today_str,
        "end_date": today_str
    }
    return _get_json(url, params)


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
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


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
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS apod (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                date TEXT,
                explanation TEXT,
                image_url TEXT
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS neo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                count INTEGER,
                smallest REAL,
                largest REAL
            );
        """)
        conn.commit()
    finally:
        conn.close()


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
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO apod (title, date, explanation, image_url)
            VALUES (?, ?, ?, ?);
        """, (
            data.get("title"),
            data.get("date"),
            data.get("explanation"),
            data.get("url"),
        ))
        conn.commit()
    finally:
        conn.close()


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
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO neo (date, count, smallest, largest)
            VALUES (?, ?, ?, ?);
        """, (neo_date, count, smallest, largest))
        conn.commit()
    finally:
        conn.close()


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
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, title, date, explanation, image_url
            FROM apod
            ORDER BY id DESC
            LIMIT 1;
        """)
        row = cur.fetchone()
        return dict(row) if row is not None else None
    finally:
        conn.close()


# --------- NETWORK HELPERS ----------
def _get_json(url: str, params: dict) -> dict:
    """
    Make a GET request to `url` with `params` and return the parsed JSON.

    Raises urllib.error.HTTPError for non-200 responses and
    json.JSONDecodeError when response body isn't valid JSON.
    """
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    # Use an unverified SSL context here so local environments without
    # up-to-date cert bundles can still run the script during development.
    # (Not for production.)
    ctx = ssl._create_unverified_context()
    with urllib.request.urlopen(url, context=ctx) as fh:
        body = fh.read()
        # decode bytes and parse JSON
        text = body.decode("utf-8")
        return json.loads(text)


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
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, date, count, smallest, largest
            FROM neo
            ORDER BY id DESC
            LIMIT 1;
        """)
        row = cur.fetchone()
        return dict(row) if row is not None else None
    finally:
        conn.close()

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
    template_path = Path(TEMPLATE_HTML)
    if not template_path.exists():
        raise FileNotFoundError(f"Template HTML file not found: {TEMPLATE_HTML}")
    
    html_content = template_path.read_text(encoding="utf-8")
    
    replacements = {
        "{{apod_title}}": html.escape(apod_row['title']),
        "{{apod_date}}": html.escape(apod_row['date']),
        "{{apod_explanation}}": html.escape(apod_row['explanation']),
        "{{apod_image_url}}": html.escape(apod_row['image_url']),
        "{{neo_count}}": str(neo_row['count']),
        "{{neo_smallest}}": f"{neo_row['smallest']:.3f}",
        "{{neo_largest}}": f"{neo_row['largest']:.3f}",
    }
    for placeholder, value in replacements.items():
        html_content = html_content.replace(placeholder, value)
    return html_content


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
    # 1. Make sure tables exist
    init_db()

    # 2. Load API key
    api_key = load_api_key()

    # ----- APOD DISABLED (partner will implement later) -----
    # apod_json = fetch_apod(api_key)
    # save_apod_to_db(apod_json)

    # 3. NEO: fetch + compute summary + save
    neo_json = fetch_neo(api_key)
    today_str = date.today().isoformat()

    neo_list = neo_json.get("near_earth_objects", {}).get(today_str, [])
    count = len(neo_list)

    if count == 0:
        smallest = 0.0
        largest = 0.0
    else:
        diam_mins = []
        diam_maxs = []
        for neo in neo_list:
            km_info = neo.get("estimated_diameter", {}).get("kilometers", {})
            dmin = km_info.get("estimated_diameter_min")
            dmax = km_info.get("estimated_diameter_max")
            if dmin is not None:
                diam_mins.append(dmin)
            if dmax is not None:
                diam_maxs.append(dmax)
        smallest = min(diam_mins) if diam_mins else 0.0
        largest  = max(diam_maxs) if diam_maxs else 0.0

    save_neo_to_db(today_str, count, smallest, largest)

    # ----- APOD DISABLED -----
    apod_row = {
        "title": "APOD not implemented yet",
        "date": "N/A",
        "explanation": "Your partner is working on this section.",
        "image_url": ""
    }

    neo_row = latest_neo()

    if neo_row is None:
        raise RuntimeError("Failed to retrieve latest NEO data from database.")

    # 4. Render HTML
    html_output = render_html(apod_row, neo_row)
    Path(OUTPUT_HTML).write_text(html_output, encoding="utf-8")

    print(f"NEO-only mode: Successfully updated {OUTPUT_HTML} and database.db.")
    # init_db()
    # api_key = load_api_key()
    
    # apod_json = fetch_apod(api_key)
    # save_apod_to_db(apod_json)
    
    # neo_json = fetch_neo(api_key)
    # today_str = date.today().isoformat()
    
    # neo_list = neo_json.get("near_earth_objects", {}).get(today_str, [])
    # count = len(neo_list)
    
    # if count == 0:
    #     smallest = 0.0
    #     largest = 0.0
    # else:
    #     diam_mins = []
    #     diam_maxs = []
    #     for neo in neo_list:
    #         km_info = neo.get("estimated_diameter", {}).get("kilometers", {})
    #         dmin = km_info.get("estimated_diameter_min", 0.0)
    #         dmax = km_info.get("estimated_diameter_max", 0.0)
    #         if dmin is not None:
    #             diam_mins.append(dmin)
    #         if dmax is not None:
    #             diam_maxs.append(dmax)
    #     smallest = min(diam_mins) if diam_mins else 0.0
    #     largest = max(diam_maxs) if diam_maxs else 0.0
        
    #     save_neo_to_db(today_str, count, smallest, largest)
        
    # apod_row = latest_apod()
    # neo_row = latest_neo()
    
    # if apod_row is None or neo_row is None:
    #     raise RuntimeError("Failed to retrieve latest data from database.")
    
    # html_output = render_html(apod_row, neo_row)
    # Path(OUTPUT_HTML).write_text(html_output, encoding="utf-8")
    # print(f"Successfully updated {OUTPUT_HTML} with latest NASA data.")

# ---------- ENTRY ----------
if __name__ == "__main__":
    process()