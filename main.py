import os
import uuid
import shutil

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

from analysis import validate_columns, get_available_filters, run_analysis

# ─────────────────────────────────────────────
# APP SETUP
# ─────────────────────────────────────────────

app = FastAPI(
    title="BikeShare Analyzer API",
    description="Backend API for the BikeShare Data Analyzer",
    version="1.0.0"
)

# ─────────────────────────────────────────────
# CORS — Allows Lovable frontend to talk to this API
# ─────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://bike-share.lovable.app"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# TEMP UPLOAD FOLDER
# ─────────────────────────────────────────────

UPLOAD_FOLDER = "temp_uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# ─────────────────────────────────────────────
# HEALTH CHECK ENDPOINT
# ─────────────────────────────────────────────

@app.get("/health")
def health_check():
    """
    Simple endpoint to confirm the server is alive.
    Lovable UI can ping this on load to check connectivity.
    """
    return {"status": "ok", "message": "BikeShare API is running!"}

# ─────────────────────────────────────────────
# UPLOAD ENDPOINT
# ─────────────────────────────────────────────

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Receives a CSV file from the UI.
    Validates required columns.
    Returns:
      - validation result
      - available months and days (for dropdowns)
      - available optional columns (Gender, Birth Year)
      - a session file_id to reference this file in /analyze
    """

    # ── 1. Accept only CSV files ──
    if not file.filename.endswith(".csv"):
        raise HTTPException(
            status_code=400,
            detail="Only CSV files are accepted. Please upload a valid .csv file."
        )

    # ── 2. Save file temporarily with a unique ID ──
    file_id = str(uuid.uuid4())
    file_path = os.path.join(UPLOAD_FOLDER, f"{file_id}.csv")

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # ── 3. Read and validate ──
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except Exception:
        os.remove(file_path)
        raise HTTPException(
            status_code=400,
            detail="Could not read the file. Please make sure it is a valid CSV file."
        )

    validation = validate_columns(df)

    # ── 4. If invalid, delete file and return friendly error ──
    if not validation["valid"]:
        os.remove(file_path)
        return {
            "success": False,
            "message": validation["message"],
            "missing_columns": validation["missing_columns"]
        }

    # ── 5. Extract available filters from actual data ──
    available_filters = get_available_filters(df)

    return {
        "success":            True,
        "message":            validation["message"],
        "file_id":            file_id,
        "available_optional": validation["available_optional"],
        "available_filters":  available_filters,
        "total_rows":         len(df)
    }

# ─────────────────────────────────────────────
# ANALYZE ENDPOINT
# ─────────────────────────────────────────────

@app.post("/analyze")
async def analyze(
    file_id: str = Form(...),
    month:   str = Form(default=None),
    day:     str = Form(default=None)
):
    """
    Receives the file_id from /upload plus filter selections.
    Runs the full analysis pipeline.
    Returns all stats as structured JSON for the UI to render.
    """

    # ── 1. Locate the temp file ──
    file_path = os.path.join(UPLOAD_FOLDER, f"{file_id}.csv")

    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404,
            detail="File not found. Please upload your file again."
        )

    # ── 2. Normalize filter inputs ──
    # Treat empty strings as None (means no filter / all)
    month = month.strip() if month and month.strip() else None
    day   = day.strip()   if day   and day.strip()   else None

    # ── 3. Run the full analysis ──
    results = run_analysis(file_path, month=month, day=day)

    # ── 4. Handle analysis-level errors ──
    if results.get("error"):
        raise HTTPException(
            status_code=422,
            detail=results["message"]
        )

    return results
