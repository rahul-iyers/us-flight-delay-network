from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import airports, routes, network, propagation, airlines
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = ROOT / "data" / "processed"

# processed data file paths
AIRPORT_STATS = PROCESSED_DIR / "airport_stats.parquet"
ROUTE_STATS = PROCESSED_DIR / "route_stats.parquet"
HOURLY_DELAYS = PROCESSED_DIR / "hourly_delays.parquet"
AIRLINE_STATS = PROCESSED_DIR / "airline_stats.parquet"
NETWORK_NODES = PROCESSED_DIR / "network_nodes.parquet"
PROPAGATION = PROCESSED_DIR / "propagation_edges.parquet"
GRAPH_JSON = PROCESSED_DIR / "graph.json"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(airports.router, prefix="/airports", tags=["airports"])
app.include_router(routes.router, prefix="/routes", tags=["routes"])
app.include_router(network.router, prefix="/network", tags=["network"])
app.include_router(propagation.router, prefix="/propagation", tags=["propagation"])
app.include_router(airlines.router, prefix="/airlines", tags=["airlines"])


@app.get("/health")
def health() -> dict:
    files = {
        "airport_stats": AIRPORT_STATS.exists(),
        "route_stats": ROUTE_STATS.exists(),
        "hourly_delays": HOURLY_DELAYS.exists(),
        "airline_stats": AIRLINE_STATS.exists(),
        "network_nodes": NETWORK_NODES.exists(),
        "propagation": PROPAGATION.exists(),
        "graph_json": GRAPH_JSON.exists(),
    }
    ready = all(files.values())
    return {"status": "ready" if ready else "missing_data", "files": files}
