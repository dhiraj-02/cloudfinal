#!/usr/bin/env python3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pydantic import BaseModel
from dotenv import load_dotenv
import os

load_dotenv()

# -----------------------------
# CONFIG
# -----------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://username:password@mongo-host:27017")
MONGO_DB = os.getenv("MONGO_DB", "kafka_windows")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "unique_emails")

# -----------------------------
# Mongo Setup
# -----------------------------
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# -----------------------------
# FastAPI Setup
# -----------------------------
app = FastAPI()

# Allow frontend to call API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Pydantic Response Model
# -----------------------------
class WindowResponse(BaseModel):
    window_start: str
    window_seconds: int
    count: int
    unique_emails: list
    ingested_at: str


# -----------------------------
# API Routes
# -----------------------------
@app.get("/active-users", response_model=WindowResponse)
def get_active_users():
    """
    Returns the last inserted window from MongoDB.
    """
    data = collection.find_one(sort=[("_id", -1)])
    if not data:
        return {
            "window_start": None,
            "window_seconds": 60,
            "count": 0,
            "unique_emails": [],
            "ingested_at": None
        }

    data["_id"] = str(data["_id"])  # Convert ObjectId
    return data
