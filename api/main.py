from fastapi import FastAPI
import pandas as pd

app = FastAPI()

# =========================
# PATHS
# =========================

TOP_GENRES_PATH = "data/gold/top_genres/"
TOP_MOVIES_PATH = "data/gold/top_movies_global/"
STATS_YEAR_PATH = "data/gold/stats_year/"

# =========================
# HELP FUNCTION
# =========================

def read_parquet_latest(path):
    import os

    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".parquet"):
                file_path = os.path.join(root, file)
                return pd.read_parquet(file_path)

# =========================
# ROUTES
# =========================

@app.get("/")
def home():
    return {"message": "TMDB API running"}

@app.get("/top-genres")
def top_genres():
    df = read_parquet_latest(TOP_GENRES_PATH)
    return df.to_dict(orient="records")

@app.get("/top-movies")
def top_movies():
    df = read_parquet_latest(TOP_MOVIES_PATH)
    
    # convertir types compliqués
    df = df.astype(str)
    
    return df.to_dict(orient="records")

@app.get("/stats-year")
def stats_year():
    df = read_parquet_latest(STATS_YEAR_PATH)
    return df.to_dict(orient="records")