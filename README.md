# World Economies ETL Project 🌍

This project demonstrates a simple **ETL (Extract, Transform, Load)** pipeline in Python.  
It scrapes GDP data by country from a Wikipedia snapshot, cleans it, and loads it into both **JSON** and **SQLite** (when run locally).

---

## 🚀 Project Evolution
This repository contains **two versions** of the ETL pipeline:

### 1. Basic Script (legacy)
- File: `legacy/etl_script_basic.py`
- A single monolithic Python script.
- Straightforward but harder to maintain or reuse.
- Useful as a starting point to understand the ETL logic step-by-step.

### 2. Function-Based Pipeline (refactored)
- File: `src/etl_pipeline.py`
- Organized into modular functions:
  - `get_html_as_BeautifulSoup()` → Extract HTML and parse with BeautifulSoup
  - `extract_DataFrame_table_from_html()` → Parse the GDP table into a DataFrame
  - `transforming()` → Clean/transform the data (remove commas, convert to float, round to 2 dp)
  - `load_dataframe_toJSON()` → Save output as JSON (local only, not in repo)
  - `load_data_to_sql()` → Save to SQLite database (local only, not in repo)
  - `log_progress()` → Track ETL stages in a log file (local only, not in repo)
- Easier to test, debug, and extend.
- Includes logging and safer DB handling.

---

## 🛠️ Requirements
- Python 3.9+  
- Libraries:
  ```bash
  pip install pandas requests beautifulsoup4

