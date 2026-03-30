from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import time

# Import your files
from Exact_engine import ExactEngine
from benchmark.benchmark import calculate_metrics

app = FastAPI(title="WT'26 AQP Engine API")

# Initialize the engine once when the server starts
# Make sure your generate_data.py has actually created this file!
exact_engine = ExactEngine(db_path="data/ecommerce.parquet")
# --- Request Models (Data Validation) ---
class QueryRequest(BaseModel):
    query_type: str  # e.g., "COUNT", "SUM", "AVG"
    column: str      # e.g., "amount"
    group_by: Optional[str] = None
    accuracy_target: Optional[float] = 0.95  # The slider value from frontend

# --- API Endpoints ---

@app.get("/")
def read_root():
    return {"status": "AQP Engine is running!"}

@app.post("/api/query/exact")
def run_exact_query(request: QueryRequest):
    """Endpoint just to test the Exact Engine by itself."""
    try:
        result, exec_time = exact_engine.run_query(
            request.query_type, request.column, request.group_by
        )
        return {"result": result, "time_ms": round(exec_time, 4)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/benchmark")
def run_full_benchmark(request: QueryRequest):
    """
    The main endpoint for the Comparison UI.
    Runs both engines and calculates the score.
    """
    try:
        # 1. Run Your Exact Engine
        exact_res, exact_time = exact_engine.run_query(
            request.query_type, request.column, request.group_by
        )

        # 2. Run Approx Engine (Mocked for now)
        # In the real hackathon, you'll import Member 1's engine here.
        # For testing, we simulate an engine that is ~4x faster and 2% off.
        e_val = exact_res[0][0] if isinstance(exact_res, list) else exact_res
        
        # MOCK APPROX RESULTS:
        approx_res = float(e_val) * 0.98 
        approx_time = exact_time / 4.2 

        # 3. Calculate the metrics using your benchmark.py function
        metrics = calculate_metrics(exact_res, approx_res, exact_time, approx_time)
        
        return metrics
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))