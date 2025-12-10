# Occupy Space – NASA API Data Viewer (Python + HTML + SQL)

## What it does
Fetches today’s NASA Astronomy Picture of the Day (APOD) and Near-Earth Object (NeoWs) data, stores them in SQLite, and writes a **static** `index.html` page you can open in any browser.  
## File tree 

occupy-space/
├── main.py              # driver (stub template you are filling)
├── database.db          # created automatically
├── templates/index.html # {{placeholder}} template
├── static/style.css     # dark-theme styles
├── config/
│   └── nasa_api_key.txt # ← key here
└── README.md            # this file


## 1. API key
Drop your 40-character NASA key inside `config/nasa_api_key.txt` **with no extra spaces**.

## 2. Install nothing
The stub uses only Python ≥ 3.9 stdlib.  
(When you add pandas / matplotlib later, install them with `pip install pandas matplotlib seaborn plotly`.)

## 3. Run
bash
cd occupy-space
python main.py          # generates database.db + index.html


## 4. View
Open the freshly-created `index.html` in your browser.  
Refresh after each run to see new data.

## 5. SQL schema
The script auto-creates:

sql
CREATE TABLE apod (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    date TEXT,
    explanation TEXT,
    image_url TEXT
);

CREATE TABLE neo (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    count INTEGER,
    smallest REAL,
    largest REAL
);


## 6. Next steps 
Fill every `pass` in `main.py` following the detailed doc-strings.  
Once the stub pipeline works, swap in your longer date ranges, bulk inserts, and fancy plots.
