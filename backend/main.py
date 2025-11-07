from fastapi import FastAPI, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import subprocess, os, yaml

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

app = FastAPI(title="ETL FoxPro → SQL Server")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.post("/api/upload_dbf")
async def upload_dbf(file: UploadFile):
    path = os.path.join(DATA_DIR, file.filename)
    with open(path, "wb") as f:
        f.write(await file.read())
    return {"status": "ok", "file": file.filename}

@app.post("/api/update_config")
async def update_config(config: dict):
    with open(os.path.join(BASE_DIR, "config.yaml"), "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f)
    return {"status": "ok", "saved": True}

@app.get("/api/discover_schema")
def discover_schema():
    try:
        subprocess.run(["python", "discover_schema.py"], cwd=BASE_DIR, check=True)
        path = os.path.join(BASE_DIR, "schema.json")
        return FileResponse(path, media_type="application/json", filename="schema.json")
    except subprocess.CalledProcessError as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/download_schema_sql")
def download_schema_sql():
    path = os.path.join(BASE_DIR, "schema.sql")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/plain", filename="schema.sql")
    return JSONResponse({"error": "schema.sql no encontrado"}, status_code=404)

@app.get("/api/run_etl")
def run_etl():
    try:
        subprocess.run(["python", "etl_main.py"], cwd=BASE_DIR, check=True)
        return {"status": "ok", "msg": "ETL ejecutado correctamente"}
    except subprocess.CalledProcessError as e:
        return JSONResponse({"error": str(e)}, status_code=500)
