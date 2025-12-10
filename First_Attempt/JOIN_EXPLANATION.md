# NEO ↔ APOD Database Joins Explained

Your database now supports **three different strategies** to link Near-Earth Objects (NEOs) with Astronomy Picture of the Day (APOD) images!

## The Concept

The idea is: **If an APOD image was taken on the same day as NEOs passed Earth, that picture might show those objects in space.**

---

## Three Joining Strategies

### 1. **Date-Based Joining** (Primary Method)
- **How it works**: Links NEO records to APOD images taken on the **exact same date**
- **Implementation**: 
  - `neo.apod_id` column stores the APOD id for that date
  - `neo_items.apod_id` column stores the APOD id for that date
- **Function**: `link_neo_to_apod_by_date()` and `link_neo_items_to_apod_by_date()`
- **Result**: 196 NEO items matched to APOD images by date (as of last run)

### 2. **Fuzzy Text Matching** (Secondary Method)
- **How it works**: Compares the NEO object name against APOD titles and descriptions using token overlap similarity
- **Example**: If a NEO is named "M77 fragment" and APOD has "M77: Spiral Galaxy", they might match
- **Implementation**: 
  - Tokenizes both texts (breaks into words)
  - Calculates Jaccard similarity (intersection / union of tokens)
  - Stores top matches with score ≥ 0.15 in `neo_item_apod_map` table
- **Function**: `fuzzy_match_neo_items_to_apod(threshold=0.15, top_k=3)`
- **Customizable**: 
  - `threshold`: Minimum similarity score (0.0-1.0) to consider a match
  - `top_k`: How many top matches to keep per NEO item

### 3. **Mapping Table** (Flexible Storage)
- **Table**: `neo_item_apod_map`
- **Purpose**: Stores the results of fuzzy matching with similarity scores
- **Columns**: 
  - `neo_item_id`: Which NEO item
  - `apod_id`: Which APOD image
  - `score`: How confident the match is (0.0-1.0)
- **Function**: `export_neo_item_apod_map()`

---

## Output Files

After running `main.py`, you get:

### `neo_apod_matched.csv` ⭐ **Main Join Result**
Contains all matched NEO↔APOD pairs with:
- NEO details: name, date, diameter range, hazard status
- APOD details: title, image URL, full explanation
- Match metadata: type (date_match/fuzzy_match), score

### `neo_item_apod_map.csv`
Raw fuzzy matching results with similarity scores

### `neo_items.csv`
All individual NEO records (with `apod_id` now populated)

### `neo_summary.csv` & `neo_stats.json`
Daily NEO summaries with linked APOD ids

---

## Database Schema

```
┌─────────────┐         ┌──────────────┐
│   apod      │◄────────│   neo        │
│ ═══════     │         │ ═══════      │
│ id (PK)     │─ apod_id ← id (PK)    │
│ title       │         │ date        │
│ date        │         │ count       │
│ explanation │         │ apod_id FK  │
│ image_url   │         └──────────────┘
└─────────────┘
       ▲
       │ ┌──────────────────┐
       │ │  neo_items       │
       │ │ ═══════          │
       └─┼─ apod_id FK      │
         │ neo_summary_id FK│
         │ neo_id           │
         │ date             │
         │ name             │
         │ diameter_min/max │
         │ is_hazardous     │
         └──────────────────┘
              │
              │ ┌─────────────────────────┐
              │ │ neo_item_apod_map       │
              └─► ═══════════════         │
                  neo_item_id FK          │
                  apod_id FK              │
                  score                   │
                  └─────────────────────────┘
```

---

## How to Use

### Run the full pipeline (all joins):
```bash
python3 main.py
```
This will:
1. Fetch fresh APOD and NEO data
2. Save to database
3. Link by date
4. Fuzzy match NEO names to APOD content
5. Export all results to CSV files

### Query the joins in Python:
```python
from main import get_joined_neo_apod_data

matches = get_joined_neo_apod_data()
for match in matches:
    print(f"{match['neo_name']} → {match['apod_title']}")
    print(f"  Match type: {match['match_type']}, Score: {match['score']}")
```

### View the results:
- **Excel/Google Sheets**: Open `neo_apod_matched.csv`
- **Command line**: `cat neo_apod_matched.csv` or `head neo_apod_matched.csv`

---

## Customization

### Adjust fuzzy matching sensitivity:
Edit the line in `process()`:
```python
fuzzy_matches = fuzzy_match_neo_items_to_apod(threshold=0.15, top_k=3)
```
- **Lower threshold** (e.g., 0.10) = More matches (less strict)
- **Higher threshold** (e.g., 0.25) = Fewer matches (more strict)
- **Higher top_k** = More potential matches per NEO item

### Build historical joins (multiple dates):
Use `populate_neo_date_range()` to fetch NEOs across a date range first:
```python
from datetime import date
start = date(2025, 11, 1)
end = date(2025, 12, 2)
populate_neo_date_range(api_key, start, end)
```
Then the joins will work across all those dates.

---

## Example Output

```
NEO Item: (2011 GO27)
  ├─ Date: 2025-12-02
  ├─ Size: 0.42-0.94 km diameter
  └─ APOD Match (date): M77: Spiral Galaxy with an Active Center
     ├─ Date: 2025-12-02  ✓ (same date!)
     ├─ Image: https://apod.nasa.gov/apod/image/2512/M77_Hubble_960.jpg
     └─ Score: 1.000 (perfect match by date)
```

---

## Notes

- **Date matches are exact** (score = 1.0) because they're based on calendar dates
- **Fuzzy matches are probabilistic** (0.0-1.0) because they're based on text similarity
- Currently getting **196 date-based matches** because today's APOD and today's NEO data happen to align
- **No fuzzy matches** were found because NEO names don't typically appear in APOD descriptions
- To get more matches, consider collecting data across multiple dates before running fuzzy matching
