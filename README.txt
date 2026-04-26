Visualizing Delay Propagation in the U.S. Flight Network

Team 21:
Michael Chian
Rahul Iyer
Sam Sukendro
Shaunak Warty


An interactive visualization of delay patterns and propagation across the U.S.
flight network using 26 million flight records from 2022–2025.


============================================================
STEPS TO RUN
============================================================

1. PREREQUISITES

Make sure Python and Node.js are installed.

Versions used:
- Python 12
- Node.js 24


2. GET THE DATA

Download the flight dataset CSV and place it in:

data/raw/flight_data.csv


3. SET UP THE PYTHON ENVIRONMENT

From the project root run:

python -m venv venv
venv/Scripts/activate
pip install -r requirements.txt


4. RUN THE DATA PIPELINE

This processes the raw CSV into parquet files and the graph JSON used by
the backend.

Run python pipeline/run_all.py

You should get an output resembling this:

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

All output files are written to data/processed/

Note: You can also run individual steps directly, for example:

python pipeline/02_aggregate.py


5. START THE BACKEND

The backend must be started from the backend/ directory.

Run:

venv/Scripts/activate
cd backend
uvicorn main:app --reload --port 8000

Verify it is running by visiting:

http://localhost:8000/health

Expected response: "status": "ready" and all required files should be listed as true.


6. START THE FRONTEND

Open a separate terminal. From the project root run:

cd frontend
npm install
npm run dev

Then open http://localhost:5173 to see the visualization


============================================================
VIEWS
============================================================

1. NETWORK MAP
US map with airports as nodes (size = flight volume, color = avg delay). Zoom/pan, click any airport for detail. Toggle propagation overlay to see delay chains.

2. AIRPORT DETAIL
Per-airport stats, hourly delay bar chart, delay cause breakdown, propagation summary.

3. AIRLINE COMPARISON
Bar chart, scatter plot, and summary table comparing all carriers.


============================================================
SIDEBAR CONTROLS
============================================================

- Departure Hour Slider
  Filters node colors to show average delay only for selected hours.

- Show Propagation
  Displays dashed orange lines showing routes that frequently propagate delays.

- Click an Airport
  Opens Airport Detail view for the selected airport.