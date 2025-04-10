from fastapi import FastAPI, HTTPException
import pandas as pd
from dateutil import parser
from typing import List

app = FastAPI()

# Load CSV file
file_path = r"C:\Users\USER\OneDrive\Documents\OpenCall\SKA_data_engineer_1.csv"
df = pd.read_csv(file_path, sep=';', header=0)

# Function to compute time difference
def compute_difference(t1, t2):
    dt1 = parser.parse(t1)
    dt2 = parser.parse(t2)
    return abs(int((dt1 - dt2).total_seconds()))

@app.post("/process_task/")
def process_task(task_data: List[str]):
    try:
        results = []
        for i in range(0, len(task_data), 2):
            t1, t2 = task_data[i], task_data[i + 1]
            results.append(str(compute_difference(t1, t2)))
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing timestamps: {str(e)}")

# Run the server using:
# uvicorn server:app --reload
