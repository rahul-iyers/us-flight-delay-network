"""
FastAPI backend for the flight-delay visualisation project.

Start with:
    uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
or from the project root:
    cd backend && uvicorn main:app --reload
"""

import json
from functools import lru_cache
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from config import (
    AIRPORT_STATS, ROUTE_STATS, HOURLY_DELAYS, AIRLINE_STATS,
    MONTHLY_STATS, NETWORK_NODES, NETWORK_EDGES,
    PROPAGATION, PROP_SUMMARY, GRAPH_JSON, CORS_ORIGINS,
)
from routers import airports, routes, network, propagation, airlines

app = FastAPI(
    title="Flight Delay Propagation API",
    version="1.0.0",
    description="Serves aggregated BTS flight data for the CSE 6242 visualisation project.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(airports.router,    prefix="/airports",    tags=["airports"])
app.include_router(routes.router,      prefix="/routes",      tags=["routes"])
app.include_router(network.router,     prefix="/network",     tags=["network"])
app.include_router(propagation.router, prefix="/propagation", tags=["propagation"])
app.include_router(airlines.router,    prefix="/airlines",    tags=["airlines"])


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
