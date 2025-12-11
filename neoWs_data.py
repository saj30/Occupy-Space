"""
NeoWs API Data Collection
Collects Near Earth Object asteroid data from NASA's NeoWs API
Stores data in SQLite database with 25 items per run
Author: Isaiah Ramirez
"""

import requests
import sqlite3
from datetime import datetime, timedelta

# NASA API key 
API_KEY = "Me4x3nRxIMDsx1RPwXwfYzuLoIosBLHIt28pMdd5"
BASE_URL_FEED = "https://api.nasa.gov/neo/rest/v1/feed"
BASE_URL_LOOKUP = "https://api.nasa.gov/neo/rest/v1/neo"


# ---------- DB SETUP ----------

def create_tables():
    """
    Creates tables in the database:
    - asteroids: main asteroid data
    - orbiting_bodies: lookup table to avoid duplicate orbiting body strings
    - approaches: detailed approach data (FK to asteroids, logical FK to orbiting_bodies)
    - orbital_elements: orbital data per asteroid (1–1 with asteroids)
    """
    conn = sqlite3.connect("space_data.db")
    cur = conn.cursor()

    # Main asteroids table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS asteroids (
            id INTEGER PRIMARY KEY,
            neo_id TEXT UNIQUE,
            name TEXT,
            nasa_jpl_url TEXT,
            absolute_magnitude REAL,
            estimated_diameter_min REAL,
            estimated_diameter_max REAL,
            is_potentially_hazardous INTEGER,
            is_sentry_object INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Lookup table for orbiting bodies (Earth, Mars, etc.)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orbiting_bodies (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        )
    """)

    # Approach data table (linked to asteroids; orbiting_body_id is just an INTEGER)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS approaches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asteroid_id INTEGER,
            approach_date TEXT,
            approach_date_full TEXT,
            epoch_close_approach INTEGER,
            rel_vel_km_s REAL,
            rel_vel_km_h REAL,
            rel_vel_mph REAL,
            miss_distance_km REAL,
            miss_distance_lunar REAL,
            miss_distance_au REAL,
            miss_distance_miles REAL,
            orbiting_body_id INTEGER,
            FOREIGN KEY (asteroid_id) REFERENCES asteroids(id)
        )
    """)

    # Orbital elements table (1–1 with asteroids)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orbital_elements (
            asteroid_id INTEGER PRIMARY KEY,
            orbit_id TEXT,
            orbit_determination_date TEXT,
            eccentricity REAL,
            semi_major_axis REAL,
            inclination REAL,
            ascending_node_longitude REAL,
            perihelion_argument REAL,
            perihelion_distance REAL,
            aphelion_distance REAL,
            orbital_period REAL,
            mean_anomaly REAL,
            mean_motion REAL,
            epoch_osculation INTEGER,
            FOREIGN KEY (asteroid_id) REFERENCES asteroids(id)
        )
    """)

    conn.commit()
    conn.close()




# ---------- HELPERS ----------

def get_last_fetch_date():
    """
    Gets the last date we fetched data for (max approach_date in approaches).
    Returns None if no data exists yet.
    """
    conn = sqlite3.connect("space_data.db")
    cur = conn.cursor()

    cur.execute("SELECT MAX(approach_date) FROM approaches")
    result = cur.fetchone()[0]

    conn.close()

    if result:
        return datetime.strptime(result, "%Y-%m-%d")
    return None


def fetch_neo_data(start_date, end_date):
    """
    Fetches NEO data from NASA NeoWs feed API for a date range.
    """
    params = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "api_key": API_KEY,
    }

    resp = requests.get(BASE_URL_FEED, params=params)
    if resp.status_code == 200:
        return resp.json()
    else:
        print(f"Error fetching feed: {resp.status_code}")
        return None


def fetch_orbital_data(neo_id: str):
    """
    Fetches detailed orbital elements for a single NEO by its neo_id
    using the NeoWs lookup endpoint.
    """
    url = f"{BASE_URL_LOOKUP}/{neo_id}"
    params = {"api_key": API_KEY}
    resp = requests.get(url, params=params)

    if resp.status_code != 200:
        print(f"Warning: failed to fetch orbital data for {neo_id}: {resp.status_code}")
        return None

    data = resp.json()
    return data.get("orbital_data")


def as_float_from_dict(d: dict, key: str):
    """
    Helper: safely convert d[key] to float, returning None if not possible.
    """
    if d is None:
        return None
    v = d.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------- DATA STORAGE ----------

def store_neo_data(data):
    """
    Stores NEO data in database, limiting to 25 new asteroids per run.
    Avoids duplicates by checking neo_id.
    Returns number of new asteroids added.
    """
    if not data or "near_earth_objects" not in data:
        return 0

    conn = sqlite3.connect("space_data.db")
    cur = conn.cursor()

    items_added = 0
    max_items = 25

    # Process each date's asteroids in sorted order (for determinism)
    for date_key in sorted(data["near_earth_objects"].keys()):
        if items_added >= max_items:
            break

        asteroids = data["near_earth_objects"][date_key]

        for asteroid in asteroids:
            if items_added >= max_items:
                break

            neo_id = asteroid["id"]  # or asteroid["neo_reference_id"]

            # Check if asteroid already exists
            cur.execute("SELECT id FROM asteroids WHERE neo_id = ?", (neo_id,))
            row = cur.fetchone()

            if row:
                asteroid_id = row[0]
            else:
                # Insert new asteroid with full set of columns
                cur.execute(
                    """
                    INSERT INTO asteroids 
                    (neo_id, name, nasa_jpl_url, absolute_magnitude, 
                     estimated_diameter_min, estimated_diameter_max, 
                     is_potentially_hazardous, is_sentry_object)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        neo_id,
                        asteroid.get("name"),
                        asteroid.get("nasa_jpl_url"),
                        asteroid.get("absolute_magnitude_h"),
                        asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_min"],
                        asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_max"],
                        1 if asteroid.get("is_potentially_hazardous_asteroid") else 0,
                        1 if asteroid.get("is_sentry_object") else 0,
                    ),
                )
                asteroid_id = cur.lastrowid
                items_added += 1

                # Fetch and store orbital elements for this new asteroid
                orbital = fetch_orbital_data(neo_id)
                if orbital:
                    cur.execute(
                        """
                        INSERT OR REPLACE INTO orbital_elements (
                            asteroid_id,
                            orbit_id,
                            orbit_determination_date,
                            eccentricity,
                            semi_major_axis,
                            inclination,
                            ascending_node_longitude,
                            perihelion_argument,
                            perihelion_distance,
                            aphelion_distance,
                            orbital_period,
                            mean_anomaly,
                            mean_motion,
                            epoch_osculation
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            asteroid_id,
                            orbital.get("orbit_id"),
                            orbital.get("orbit_determination_date"),
                            as_float_from_dict(orbital, "eccentricity"),
                            as_float_from_dict(orbital, "semi_major_axis"),
                            as_float_from_dict(orbital, "inclination"),
                            as_float_from_dict(orbital, "ascending_node_longitude"),
                            as_float_from_dict(orbital, "perihelion_argument"),
                            as_float_from_dict(orbital, "perihelion_distance"),
                            as_float_from_dict(orbital, "aphelion_distance"),
                            as_float_from_dict(orbital, "orbital_period"),
                            as_float_from_dict(orbital, "mean_anomaly"),
                            as_float_from_dict(orbital, "mean_motion"),
                            as_float_from_dict(orbital, "epoch_osculation"),
                        ),
                    )

            # Store approach data (can have multiple approaches per asteroid)
            for approach in asteroid.get("close_approach_data", []):
                approach_date = approach["close_approach_date"]

                # Check if this specific approach already exists for that asteroid + date
                cur.execute(
                    """
                    SELECT id FROM approaches 
                    WHERE asteroid_id = ? AND approach_date = ?
                    """,
                    (asteroid_id, approach_date),
                )
                if cur.fetchone():
                    continue

                rel_vel = approach.get("relative_velocity", {})
                miss_dist = approach.get("miss_distance", {})
                body_name = approach.get("orbiting_body")

                # Get or create orbiting_bodies row
                orbiting_body_id = None
                if body_name:
                    cur.execute(
                        "SELECT id FROM orbiting_bodies WHERE name = ?",
                        (body_name,),
                    )
                    ob_row = cur.fetchone()
                    if ob_row:
                        orbiting_body_id = ob_row[0]
                    else:
                        cur.execute(
                            "INSERT INTO orbiting_bodies (name) VALUES (?)",
                            (body_name,),
                        )
                        orbiting_body_id = cur.lastrowid

                cur.execute(
                    """
                    INSERT INTO approaches 
                    (asteroid_id,
                     approach_date, approach_date_full, epoch_close_approach,
                     rel_vel_km_s, rel_vel_km_h, rel_vel_mph,
                     miss_distance_km, miss_distance_lunar,
                     miss_distance_au, miss_distance_miles,
                     orbiting_body_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        asteroid_id,
                        approach_date,
                        approach.get("close_approach_date_full"),
                        approach.get("epoch_date_close_approach"),
                        as_float_from_dict(rel_vel, "kilometers_per_second"),
                        as_float_from_dict(rel_vel, "kilometers_per_hour"),
                        as_float_from_dict(rel_vel, "miles_per_hour"),
                        as_float_from_dict(miss_dist, "kilometers"),
                        as_float_from_dict(miss_dist, "lunar"),
                        as_float_from_dict(miss_dist, "astronomical"),
                        as_float_from_dict(miss_dist, "miles"),
                        orbiting_body_id,
                    ),
                )

    conn.commit()
    conn.close()
    return items_added


# ---------- MAIN ----------

def main():
    """
    Main function - fetches and stores NEO data incrementally.
    Each run gets data for 7 days and stores up to 25 new asteroids.
    """
    print("=" * 50)
    print("NeoWs Data Collection")
    print("=" * 50)

    # Create tables if they don't exist
    create_tables()

    # Determine date range to fetch
    last_date = get_last_fetch_date()

    if last_date:
        start_date = last_date + timedelta(days=1)
        print(f"Last fetch date: {last_date.strftime('%Y-%m-%d')}")
    else:
        # Start from a date with good data
        start_date = datetime(2024, 1, 1)
        print("First run - starting from 2024-01-01")

    end_date = start_date + timedelta(days=7)  # 7-day range (API limit)

    print(
        f"Fetching data from {start_date.strftime('%Y-%m-%d')} "
        f"to {end_date.strftime('%Y-%m-%d')}"
    )

    # Fetch data from API
    data = fetch_neo_data(start_date, end_date)

    if data:
        items_added = store_neo_data(data)
        print(f"✓ Successfully added {items_added} new asteroids to database")

        # Show current totals
        conn = sqlite3.connect("space_data.db")
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM asteroids")
        total_asteroids = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM approaches")
        total_approaches = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM orbital_elements")
        total_orbital = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM orbiting_bodies")
        total_bodies = cur.fetchone()[0]
        conn.close()

        print(f"Total asteroids in database:       {total_asteroids}")
        print(f"Total approaches in database:      {total_approaches}")
        print(f"Total orbital_elements rows:       {total_orbital}")
        print(f"Total orbiting_bodies (distinct):  {total_bodies}")
        print("\nRun this script again to fetch more data!")
    else:
        print("Failed to fetch data from API")

    print("=" * 50)


if __name__ == "__main__":
    main()
