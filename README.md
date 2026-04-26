# Visualizing Delay Propagation in the U.S. Flight Network

Team 21: Michael Chian, Rahul Iyer, Sam Sukendro, Shaunak Warty

An interactive visualization of delay patterns and propagation across the U.S. flight network using 26 million flight records from 2022–2025.

## Steps to Run

### 1. Prerequisites

Make sure you have Python and Node.js installed. We were using Python 12 and Node.js 24.

### 2. Get the data

Download the flight dataset CSV and place it in `data/raw` named `flight_data.csv`:

```
data/raw/flight_data.csv
```

### 3. Set up the Python environment

From the project root:

```bash
python -m venv venv
venv/Scripts/activate
pip install -r requirements.txt
```


### 4. Run the data pipeline

This processes the raw CSV into the parquet files and graph JSON the backend needs. This shouldn't take more than a few minutes to run.

```bash
python pipeline/run_all.py
```

You should see an output like:
```
============================================================
Step 1/4: Load & Clean
============================================================
...

Step 1 completed in 16.60 seconds.

============================================================
Step 2/4: Aggregate Stats
============================================================
...

Step 2 completed in 15.21 seconds.

etc.
```

All output files are written to `data/processed/`.

Note: you can also run individual steps by running that step's file directly (e.g. `python pipeline/02_aggregate.py`)

### 5. Start the backend

The backend must be started from the `backend/` directory.

```bash
venv/Scripts/activate
cd backend
uvicorn main:app --reload --port 8000
```

Verify it's running: [http://localhost:8000/health](http://localhost:8000/health)

You should see `"status": "ready"` with all files listed as `true`.

### 6. Start the frontend

In a **separate terminal**, from the project root:

```bash
cd frontend
npm install
npm run dev
```

Then open [http://localhost:5173](http://localhost:5173) in your browser.


## Views

| View | Description |
|------|-------------|
| **Network Map** | US map with airports as nodes (size = flight volume, color = avg delay). Zoom/pan, click any airport for detail. Toggle propagation overlay to see delay chains. |
| **Airport Detail** | Per-airport stats, hourly delay bar chart, delay cause breakdown, propagation summary. |
| **Airline Comparison** | Bar chart, scatter plot, and summary table comparing all carriers. |


