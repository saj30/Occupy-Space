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
import sqlite3
import csv
from tempfile import template
import urllib.request
import urllib.parse
from datetime import date, datetime
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
def fetch_apod(api_key: str, target_date: Optional[date] = None) -> dict:
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
    if target_date is not None:
        params["date"] = target_date.isoformat()
    return _get_json(url, params)


def fetch_neo(api_key: str, target_date: Optional[date] = None) -> dict:
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
    if target_date is None:
        target_date = date.today()
    day_str = target_date.isoformat()
    params = {
        "api_key": api_key,
        "start_date": day_str,
        "end_date": day_str,
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
        # table to store individual near earth object records (one row per object)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS neo_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                neo_id TEXT,
                date TEXT,
                name TEXT,
                diameter_min REAL,
                diameter_max REAL,
                is_hazardous INTEGER
            );
        """)
        # Ensure neo_items has neo_summary_id column for joining to neo table
        cursor.execute("PRAGMA table_info('neo_items');")
        cols = [r[1] for r in cursor.fetchall()]
        if 'neo_summary_id' not in cols:
            cursor.execute("ALTER TABLE neo_items ADD COLUMN neo_summary_id INTEGER;")
        # Ensure neo table has an apod_id column which will reference apod.id for date-based joins
        cursor.execute("PRAGMA table_info('neo');")
        neo_cols = [r[1] for r in cursor.fetchall()]
        if 'apod_id' not in neo_cols:
            cursor.execute("ALTER TABLE neo ADD COLUMN apod_id INTEGER;")
        # Ensure neo_items has an apod_id column for per-item linking
        cursor.execute("PRAGMA table_info('neo_items');")
        ni_cols = [r[1] for r in cursor.fetchall()]
        if 'apod_id' not in ni_cols:
            cursor.execute("ALTER TABLE neo_items ADD COLUMN apod_id INTEGER;")
        # create mapping table for fuzzy matches between neo_items and apod
        cursor.execute("PRAGMA table_info('neo_item_apod_map');")
        if not cursor.fetchall():
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS neo_item_apod_map (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    neo_item_id INTEGER,
                    apod_id INTEGER,
                    score REAL
                );
            ''')
        # Ensure neo_items has an apod_id column for per-item linking
        cursor.execute("PRAGMA table_info('neo_items');")
        ni_cols = [r[1] for r in cursor.fetchall()]
        if 'apod_id' not in ni_cols:
            cursor.execute("ALTER TABLE neo_items ADD COLUMN apod_id INTEGER;")
        # create mapping table for fuzzy matches between neo_items and apod
        cursor.execute("PRAGMA table_info('neo_item_apod_map');")
        if not cursor.fetchall():
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS neo_item_apod_map (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    neo_item_id INTEGER,
                    apod_id INTEGER,
                    score REAL
                );
            ''')
        conn.commit()
    finally:
        conn.close()


def save_neo_items_to_db(neo_date: str, neo_list: list, max_items: int = 25, neo_summary_id: int | None = None):
    """
    Save up to `max_items` individual NEO records for `neo_date` into
    the `neo_items` table.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        saved = 0
        for neo in neo_list:
            if saved >= max_items:
                break
            neo_id = neo.get("id")
            name = neo.get("name")
            km_info = neo.get("estimated_diameter", {}).get("kilometers", {})
            dmin = km_info.get("estimated_diameter_min")
            dmax = km_info.get("estimated_diameter_max")
            hazardous = 1 if neo.get("is_potentially_hazardous_asteroid") else 0
            cur.execute(
                """
                INSERT INTO neo_items (neo_summary_id, neo_id, date, name, diameter_min, diameter_max, is_hazardous)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (neo_summary_id, neo_id, neo_date, name, dmin, dmax, hazardous),
            )
            saved += 1
        conn.commit()
        return saved
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
        return cur.lastrowid
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
        return cur.lastrowid
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


def get_neo_summary_id_for_date(neo_date: str) -> int | None:
    """Return existing neo summary row id for the given date or None."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM neo WHERE date = ? LIMIT 1;", (neo_date,))
        row = cur.fetchone()
        return row[0] if row is not None else None
    finally:
        conn.close()

def get_latest_neo_date() -> date | None:
    """
    Return the most recent date stored in the neo table, or None if empty.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(date) FROM neo;")
        row = cur.fetchone()
        max_date_str = row[0]
        if max_date_str is None:
            return None
        return datetime.strptime(max_date_str, "%Y-%m-%d").date()
    finally:
        conn.close()


def get_apod_id_for_date(apod_date: str) -> int | None:
    """Return apod.id for a given ISO date string or None if missing."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM apod WHERE date = ? LIMIT 1;", (apod_date,))
        row = cur.fetchone()
        return row[0] if row is not None else None
    finally:
        conn.close()


def populate_apod_date_range(api_key: str, start_date: date, end_date: date) -> int:
    """Fetch APOD for each date in range and save to DB. Returns number of saved rows."""
    cur_date = start_date
    saved = 0
    while cur_date <= end_date:
        try:
            # skip if already have apod for that date
            if get_apod_id_for_date(cur_date.isoformat()) is not None:
                cur_date = cur_date.fromordinal(cur_date.toordinal() + 1)
                continue
            payload = fetch_apod(api_key, target_date=cur_date)
            # API may return errors or video-only results; we attempt to save whatever is returned.
            payload_date = payload.get("date") or cur_date.isoformat()
            save_apod_to_db(payload)
            saved += 1
        except Exception:
            # skip failed dates and keep going
            pass
        cur_date = cur_date.fromordinal(cur_date.toordinal() + 1)
    return saved


def link_neo_to_apod_by_date() -> int:
    """Link existing neo summaries to apod rows where dates match; returns count updated."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, date FROM neo;")
        rows = cur.fetchall()
        updated = 0
        for r in rows:
            nid = r[0]
            d = r[1]
            if d is None:
                continue
            apod_id = get_apod_id_for_date(d)
            if apod_id is not None:
                cur.execute("UPDATE neo SET apod_id = ? WHERE id = ?;", (apod_id, nid))
                updated += 1
        conn.commit()
        return updated
    finally:
        conn.close()


def export_neo_apod_join(outfile: str = "neo_apod_join.csv") -> str:
    """Export joined neo <> apod rows (by apod_id) to CSV for review."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT neo.id, neo.date, neo.count, neo.smallest, neo.largest,
                   apod.id as apod_id, apod.title, apod.image_url
            FROM neo LEFT JOIN apod ON neo.apod_id = apod.id
            ORDER BY neo.id ASC;
        """)
        rows = cur.fetchall()
        with open(outfile, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["neo_id","date","count","smallest","largest","apod_id","apod_title","apod_image_url"]) 
            for r in rows:
                writer.writerow([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]])
        return outfile
    finally:
        conn.close()


def export_neo_summary_csv(outfile: str = "neo_summary.csv") -> str:
    """
    Export all rows from `neo` table to CSV and return path to the file.
    Also produces a small JSON stats file (neo_stats.json) with simple aggregations.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, date, count, smallest, largest FROM neo ORDER BY id ASC;")
        rows = cur.fetchall()
        # write CSV
        with open(outfile, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["id", "date", "count", "smallest", "largest"])
            for r in rows:
                writer.writerow([r[0], r[1], r[2], r[3], r[4]])

        # compute aggregate stats and save to json
        stats = {
            "total_rows": len(rows),
            "avg_count_per_day": None,
            "min_diameter": None,
            "max_diameter": None,
        }
        if rows:
            counts = [r[2] for r in rows]
            smallest_vals = [r[3] for r in rows if r[3] is not None]
            largest_vals = [r[4] for r in rows if r[4] is not None]
            stats["avg_count_per_day"] = sum(counts) / len(counts) if counts else 0
            stats["min_diameter"] = min(smallest_vals) if smallest_vals else 0
            stats["max_diameter"] = max(largest_vals) if largest_vals else 0

        stats_path = Path("neo_stats.json")
        stats_path.write_text(json.dumps(stats, indent=2), encoding="utf-8")
        return outfile
    finally:
        conn.close()


def export_neo_items_csv(outfile: str = "neo_items.csv") -> str:
    """
    Dump the `neo_items` table to a CSV file for use in analysis / visualizations.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, neo_summary_id, neo_id, date, name, diameter_min, diameter_max, is_hazardous FROM neo_items ORDER BY id ASC;")
        rows = cur.fetchall()
        with open(outfile, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["id", "neo_summary_id", "neo_id", "date", "name", "diameter_min", "diameter_max", "is_hazardous"])
            for r in rows:
                writer.writerow([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]])
        return outfile
    finally:
        conn.close()


def get_all_neo_dates() -> list:
    """Return a sorted list of date objects present in the `neo` table."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT date FROM neo WHERE date IS NOT NULL;")
        rows = cur.fetchall()
        dates = []
        for r in rows:
            try:
                dates.append(datetime.strptime(r[0], "%Y-%m-%d").date())
            except Exception:
                pass
        dates.sort()
        return dates
    finally:
        conn.close()


def populate_apod_for_all_neo_dates(api_key: str) -> int:
    """Populate APOD rows covering the full range of dates present in `neo` table."""
    dates = get_all_neo_dates()
    if not dates:
        return 0
    start = dates[0]
    end = dates[-1]
    return populate_apod_date_range(api_key, start, end)


def link_neo_items_to_apod_by_date() -> int:
    """Link neo_items rows to apod rows by exact date match; return number updated."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, date FROM neo_items WHERE apod_id IS NULL;")
        rows = cur.fetchall()
        updated = 0
        for r in rows:
            nid = r[0]
            d = r[1]
            if d is None:
                continue
            apod_id = get_apod_id_for_date(d)
            if apod_id is not None:
                cur.execute("UPDATE neo_items SET apod_id = ? WHERE id = ?;", (apod_id, nid))
                updated += 1
        conn.commit()
        return updated
    finally:
        conn.close()


def fuzzy_match_neo_items_to_apod(threshold: float = 0.15, top_k: int = 1) -> int:
    """Compute simple token-overlap similarity between `neo_items.name` and APOD title+explanation.

    Stores top_k matches with score >= threshold into `neo_item_apod_map`.
    Returns number of mappings inserted.
    """
    import re

    def tokens(text: str) -> set:
        if not text:
            return set()
        t = re.findall(r"\w+", text.lower())
        return set(t)

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        # load APOD text cache
        cur.execute("SELECT id, title, explanation FROM apod;")
        apods = [(r[0], (r[1] or "") + " " + (r[2] or "")) for r in cur.fetchall()]
        apod_tokens = [(aid, tokens(text)) for (aid, text) in apods]

        # iterate neo_items
        cur.execute("SELECT id, name FROM neo_items;")
        items = cur.fetchall()
        inserted = 0
        for itm in items:
            item_id = itm[0]
            name = itm[1] or ""
            tset = tokens(name)
            if not tset:
                continue
            scores = []
            for aid, atoks in apod_tokens:
                if not atoks:
                    continue
                inter = tset.intersection(atoks)
                uni = tset.union(atoks)
                score = len(inter) / len(uni) if uni else 0.0
                if score >= threshold:
                    scores.append((score, aid))
            if not scores:
                continue
            scores.sort(reverse=True)
            for score, aid in scores[:top_k]:
                cur.execute("INSERT INTO neo_item_apod_map (neo_item_id, apod_id, score) VALUES (?, ?, ?);", (item_id, aid, score))
                inserted += 1
        conn.commit()
        return inserted
    finally:
        conn.close()


def export_neo_item_apod_map(outfile: str = "neo_item_apod_map.csv") -> str:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, neo_item_id, apod_id, score FROM neo_item_apod_map ORDER BY id ASC;")
        rows = cur.fetchall()
        with open(outfile, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["id", "neo_item_id", "apod_id", "score"])
            for r in rows:
                writer.writerow([r[0], r[1], r[2], r[3]])
        return outfile
    finally:
        conn.close()


def get_joined_neo_apod_data() -> list:
    """
    Query all NEO items that have been matched to APOD images (by date or fuzzy match).
    Returns a list of dicts with NEO item details linked to their matched APOD.
    
    Returns
    -------
    list
        Each dict contains:
        'neo_item_id', 'neo_name', 'neo_date', 'diameter_min', 'diameter_max', 'is_hazardous',
        'apod_id', 'apod_title', 'apod_date', 'apod_explanation', 'apod_url', 'match_type', 'score'
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        # Get NEO items linked by exact date match
        cur.execute("""
            SELECT 
                ni.id as neo_item_id,
                ni.name as neo_name,
                ni.date as neo_date,
                ni.diameter_min,
                ni.diameter_max,
                ni.is_hazardous,
                a.id as apod_id,
                a.title as apod_title,
                a.date as apod_date,
                a.explanation as apod_explanation,
                a.image_url as apod_url,
                'date_match' as match_type,
                1.0 as score
            FROM neo_items ni
            INNER JOIN apod a ON ni.apod_id = a.id
            WHERE ni.apod_id IS NOT NULL
        """)
        date_matches = cur.fetchall()

        # Get NEO items linked by fuzzy matching (name similarity)
        cur.execute("""
            SELECT 
                ni.id as neo_item_id,
                ni.name as neo_name,
                ni.date as neo_date,
                ni.diameter_min,
                ni.diameter_max,
                ni.is_hazardous,
                a.id as apod_id,
                a.title as apod_title,
                a.date as apod_date,
                a.explanation as apod_explanation,
                a.image_url as apod_url,
                'fuzzy_match' as match_type,
                nim.score
            FROM neo_items ni
            INNER JOIN neo_item_apod_map nim ON ni.id = nim.neo_item_id
            INNER JOIN apod a ON nim.apod_id = a.id
        """)
        fuzzy_matches = cur.fetchall()

        # Combine results and convert to list of dicts
        all_matches = date_matches + fuzzy_matches
        results = []
        for row in all_matches:
            results.append({
                'neo_item_id': row[0],
                'neo_name': row[1],
                'neo_date': row[2],
                'diameter_min': row[3],
                'diameter_max': row[4],
                'is_hazardous': row[5],
                'apod_id': row[6],
                'apod_title': row[7],
                'apod_date': row[8],
                'apod_explanation': row[9],
                'apod_url': row[10],
                'match_type': row[11],
                'score': row[12]
            })
        return results
    finally:
        conn.close()


def export_joined_neo_apod_csv(outfile: str = "neo_apod_matched.csv") -> str:
    """
    Export all matched NEO items and their corresponding APOD images to a CSV file.
    This shows which near-Earth objects might have been photographed by APOD.
    """
    matches = get_joined_neo_apod_data()
    with open(outfile, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "neo_item_id", "neo_name", "neo_date", "diameter_min_km", "diameter_max_km", "is_hazardous",
            "apod_id", "apod_title", "apod_date", "apod_url", "match_type", "match_score"
        ])
        for match in matches:
            writer.writerow([
                match['neo_item_id'],
                match['neo_name'],
                match['neo_date'],
                match['diameter_min'],
                match['diameter_max'],
                match['is_hazardous'],
                match['apod_id'],
                match['apod_title'],
                match['apod_date'],
                match['apod_url'],
                match['match_type'],
                f"{match['score']:.3f}"
            ])
    return outfile


def get_neo_items_for_apod(apod_id: int) -> list:
    """
    Get all NEO items matched to a specific APOD image.
    Useful for seeing: 'What NEOs appeared on the same day as this APOD picture?'
    
    Parameters
    ----------
    apod_id : int
        The ID of the APOD record
    
    Returns
    -------
    list
        List of dicts with NEO item data
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        # Get NEO items linked by exact date match
        cur.execute("""
            SELECT 
                ni.id, ni.name, ni.date, ni.diameter_min, ni.diameter_max, 
                ni.is_hazardous, 'date_match' as match_type, 1.0 as score
            FROM neo_items ni
            WHERE ni.apod_id = ?
            UNION ALL
            SELECT 
                ni.id, ni.name, ni.date, ni.diameter_min, ni.diameter_max, 
                ni.is_hazardous, 'fuzzy_match' as match_type, nim.score
            FROM neo_items ni
            INNER JOIN neo_item_apod_map nim ON ni.id = nim.neo_item_id
            WHERE nim.apod_id = ?
            ORDER BY score DESC
        """, (apod_id, apod_id))
        
        results = []
        for row in cur.fetchall():
            results.append({
                'id': row[0],
                'name': row[1],
                'date': row[2],
                'diameter_min': row[3],
                'diameter_max': row[4],
                'is_hazardous': bool(row[5]),
                'match_type': row[6],
                'score': row[7]
            })
        return results
    finally:
        conn.close()


def get_apod_for_neo_item(neo_item_id: int) -> dict | None:
    """
    Get the APOD image(s) matched to a specific NEO item.
    Useful for seeing: 'What APOD picture was taken when this NEO passed Earth?'
    
    Parameters
    ----------
    neo_item_id : int
        The ID of the NEO item
    
    Returns
    -------
    dict | None
        APOD data if found, None otherwise
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        # First try date-based match
        cur.execute("""
            SELECT a.id, a.title, a.date, a.explanation, a.image_url
            FROM apod a
            INNER JOIN neo_items ni ON a.id = ni.apod_id
            WHERE ni.id = ?
        """, (neo_item_id,))
        
        row = cur.fetchone()
        if row:
            return {
                'id': row[0],
                'title': row[1],
                'date': row[2],
                'explanation': row[3],
                'image_url': row[4],
                'match_type': 'date_match'
            }
        
        # Try fuzzy match
        cur.execute("""
            SELECT a.id, a.title, a.date, a.explanation, a.image_url, nim.score
            FROM apod a
            INNER JOIN neo_item_apod_map nim ON a.id = nim.apod_id
            WHERE nim.neo_item_id = ?
            ORDER BY nim.score DESC
            LIMIT 1
        """, (neo_item_id,))
        
        row = cur.fetchone()
        if row:
            return {
                'id': row[0],
                'title': row[1],
                'date': row[2],
                'explanation': row[3],
                'image_url': row[4],
                'match_type': 'fuzzy_match',
                'score': row[5]
            }
        
        return None
    finally:
        conn.close()


def print_join_summary() -> None:
    """Print a human-readable summary of all joins in the database."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        print("\n" + "="*70)
        print("NEO ↔ APOD JOIN SUMMARY")
        print("="*70)
        
        # Count date-based joins
        cur.execute("SELECT COUNT(*) FROM neo_items WHERE apod_id IS NOT NULL;")
        date_joins = cur.fetchone()[0]
        print(f"\n📅 Date-Based Joins (neo_items → apod by exact date):")
        print(f"   {date_joins} NEO items linked to APOD images")
        
        # Count fuzzy joins
        cur.execute("SELECT COUNT(*) FROM neo_item_apod_map;")
        fuzzy_joins = cur.fetchone()[0]
        print(f"\n🔤 Fuzzy Text Matches (neo_items → apod by name similarity):")
        print(f"   {fuzzy_joins} potential matches by text similarity")
        
        # Count unique APODs involved
        cur.execute("SELECT COUNT(DISTINCT apod_id) FROM neo_items WHERE apod_id IS NOT NULL;")
        unique_apods = cur.fetchone()[0]
        print(f"\n🖼️  Unique APOD Images Matched:")
        print(f"   {unique_apods} different APOD pictures linked to NEOs")
        
        # Show date range
        cur.execute("SELECT MIN(date), MAX(date) FROM neo_items;")
        min_date, max_date = cur.fetchone()
        print(f"\n📆 Date Range in Database:")
        print(f"   {min_date} to {max_date}")
        
        # Show sample APOD with most NEOs
        cur.execute("""
            SELECT a.title, a.date, COUNT(ni.id) as neo_count
            FROM apod a
            LEFT JOIN neo_items ni ON a.id = ni.apod_id
            GROUP BY a.id
            ORDER BY neo_count DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            print(f"\n⭐ APOD with Most NEOs:")
            print(f"   \"{row[0]}\" ({row[1]})")
            print(f"   {row[2]} near-Earth objects on this date")
        
        print("\n" + "="*70 + "\n")
    finally:
        conn.close()


def populate_neo_date_range(api_key: str, start_date: date, end_date: date, per_day_limit: int = 25):
    """
    Helper to fetch and store NEOs across a date range (inclusive).

    - Calls fetch_neo(api_key, target_date) for each date in the range
    - Stores the daily summary to `neo` and up to `per_day_limit` items to `neo_items`

    Use this to build a large `neo_items` table for your final deliverable (e.g., gather 100+ rows).
    """
    cur_date = start_date
    added = 0
    while cur_date <= end_date:
        try:
            payload = fetch_neo(api_key, target_date=cur_date)
            day_str = cur_date.isoformat()
            neo_list = payload.get("near_earth_objects", {}).get(day_str, [])
            count = len(neo_list)
            if count > 0:
                diam_mins = [ n.get("estimated_diameter", {}).get("kilometers", {}).get("estimated_diameter_min") for n in neo_list ]
                diam_maxs = [ n.get("estimated_diameter", {}).get("kilometers", {}).get("estimated_diameter_max") for n in neo_list ]
                smallest = min([v for v in diam_mins if v is not None], default=0.0)
                largest = max([v for v in diam_maxs if v is not None], default=0.0)
            else:
                smallest = largest = 0.0

            # avoid creating duplicate daily summaries
            summary_id = get_neo_summary_id_for_date(day_str)
            if summary_id is None:
                summary_id = save_neo_to_db(day_str, count, smallest, largest)
            saved_items = save_neo_items_to_db(day_str, neo_list, max_items=per_day_limit, neo_summary_id=summary_id)
            added += saved_items
        except Exception:
            # continue on network or parsing errors
            pass
        cur_date = cur_date.fromordinal(cur_date.toordinal() + 1)
    return added


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

    # ----- APOD: fetch + save (enabled) -----
    try:
        apod_json = fetch_apod(api_key, target_date=date.today())
        # avoid duplicate apod rows for the same date
        apod_existing = get_apod_id_for_date(apod_json.get("date", date.today().isoformat()))
        if apod_existing is None:
            save_apod_to_db(apod_json)
    except Exception:
        apod_json = None

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

    # Save the daily summary (and get the db id so items can reference it)
    # avoid duplicate daily summaries for today
    summary_id = get_neo_summary_id_for_date(today_str)
    if summary_id is None:
        summary_id = save_neo_to_db(today_str, count, smallest, largest)
    # Also save up to 25 individual NEO records for later analysis / visualizations
    try:
        saved_items = save_neo_items_to_db(today_str, neo_list, max_items=25, neo_summary_id=summary_id)
    except Exception:
        # Non-fatal — continue if inserting individual items fails
        saved_items = 0

    # export summaries and stats for teacher deliverable / analysis
    try:
        csv_path = export_neo_summary_csv("neo_summary.csv")
    except Exception:
        csv_path = None

    # export neo_items for analysis
    try:
        items_csv = export_neo_items_csv("neo_items.csv")
    except Exception:
        items_csv = None

    # Link neo summaries to apod rows by matching dates (fills neo.apod_id)
    try:
        linked = link_neo_to_apod_by_date()
        print(f"Linked {linked} NEO summaries to APOD by date")
    except Exception as e:
        linked = 0
        print(f"Date-based NEO→APOD linking failed: {e}")

    # Link individual neo_items to apod by date (fills neo_items.apod_id)
    try:
        items_linked = link_neo_items_to_apod_by_date()
        print(f"Linked {items_linked} NEO items to APOD by date")
    except Exception as e:
        items_linked = 0
        print(f"NEO items→APOD linking failed: {e}")

    # Perform fuzzy matching between neo_items and apod based on name/title similarity
    try:
        fuzzy_matches = fuzzy_match_neo_items_to_apod(threshold=0.15, top_k=3)
        print(f"Found {fuzzy_matches} fuzzy matches between NEO items and APOD")
    except Exception as e:
        fuzzy_matches = 0
        print(f"Fuzzy matching failed: {e}")

    # Export the mapping results
    try:
        map_csv = export_neo_item_apod_map("neo_item_apod_map.csv")
        print(f"Exported NEO↔APOD mappings to {map_csv}")
    except Exception as e:
        print(f"Failed to export mappings: {e}")

    # Export the final joined data (NEO items with their matched APODs)
    try:
        joined_csv = export_joined_neo_apod_csv("neo_apod_matched.csv")
        matches = get_joined_neo_apod_data()
        print(f"Exported {len(matches)} matched NEO↔APOD pairs to {joined_csv}")
        if matches:
            print("\nSample matches (NEO → APOD):")
            for match in matches[:3]:  # Show first 3 matches
                print(f"  • {match['neo_name']} ({match['neo_date']}) → {match['apod_title']} [{match['match_type']}]")
    except Exception as e:
        print(f"Failed to export joined data: {e}")

    # Use latest APOD row if available for the rendered page
    apod_row = latest_apod() or {
        "title": "APOD not available",
        "date": "N/A",
        "explanation": "No APOD entry found.",
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