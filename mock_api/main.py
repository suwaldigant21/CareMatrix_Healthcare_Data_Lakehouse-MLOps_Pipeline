# pyrefly: ignore [missing-import]
from fastapi import FastAPI, HTTPException, Security, Query
# pyrefly: ignore [missing-import]
from fastapi.security.api_key import APIKeyHeader
import math
import os
from typing import Optional

import pandas as pd

app = FastAPI(title="CareMatrix Mock Claims API")

# Dev-only default for the local synthetic-data demo. Override via the
# CAREMATRIX_API_KEY environment variable in a real deployment.
API_KEY = os.getenv("CAREMATRIX_API_KEY", "carematrix-demo-token")
API_KEY_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Could not validate credentials")
    return api_key

# Load data globally into memory on startup
try:
    # dtype=str prevents ID truncation; fillna("") prevents JSON serialization crashes
    df = pd.read_csv("inpatient_claims.csv", dtype=str).fillna("")
    
    # --- UPGRADE: Simulate an 'updated_at' timestamp for delta loads ---
    if 'updated_at' not in df.columns:
        date_range = pd.date_range(start='2026-01-01', periods=len(df), freq='min')
        df['updated_at'] = date_range.strftime('%Y-%m-%dT%H:%M:%S')
        
except Exception as e:
    print(f"Error loading CSV: {e}")
    df = pd.DataFrame()

@app.get("/api/v1/claims")
def get_claims(
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=1000),
    updated_after: Optional[str] = Query(None, description="Filter claims updated after this ISO 8601 timestamp"),
    authenticated: str = Security(verify_api_key)
):
    if df.empty:
        return {"data": [], "page": page, "total_pages": 0, "total_records": 0}

    # Filter logic (direct reference rather than copying the whole DF)
    if updated_after:
        filtered_df = df[df['updated_at'] > updated_after]
    else:
        filtered_df = df

    total_records = len(filtered_df)
    total_pages = math.ceil(total_records / limit) if total_records > 0 else 1
    
    # Pagination Logic
    start_idx = (page - 1) * limit
    end_idx = start_idx + limit
    
    # Slice and convert to JSON-safe dictionary
    data = filtered_df.iloc[start_idx:end_idx].to_dict(orient="records")

    return {
        "page": page,
        "limit": limit,
        "total_pages": total_pages,
        "total_records": total_records,
        "data": data
    }