from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import sqlite3
import os
from pymongo import MongoClient
from datetime import datetime

app = FastAPI(title="Wine Quality API", version="1.0.0")

# SQLite Database connection - fixed path
def get_sql_connection():
    db_path = os.path.join(os.path.dirname(__file__), '..', 'wine_quality.db')
    return sqlite3.connect(db_path)

# MongoDB connection
mongo_client = MongoClient("mongodb+srv://dkudum_db_user:GPLusKiPgfcTJQBJ@cluster0.lpotyed.mongodb.net/?appName=Cluster0")
mongo_db = mongo_client['wine_quality_db']

# Pydantic models
class WineCreate(BaseModel):
    name: str
    region: str
    vintage: int

# Simple root endpoint
@app.get("/")
def read_root():
    return {"message": "Wine Quality API is running!"}

# CRUD Operations - SQL Database
@app.post("/wines/", response_model=dict)
def create_wine(wine: WineCreate):
    conn = get_sql_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO wines (name, region, vintage) VALUES (?, ?, ?)', 
                  (wine.name, wine.region, wine.vintage))
    wine_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return {"message": "Wine created successfully", "wine_id": wine_id}

@app.get("/wines/", response_model=List[dict])
def read_wines():
    conn = get_sql_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM wines')
    wines = cursor.fetchall()
    conn.close()
    return [{"wine_id": row[0], "name": row[1], "region": row[2], "vintage": row[3]} for row in wines]

@app.put("/wines/{wine_id}")
def update_wine(wine_id: int, wine: WineCreate):
    conn = get_sql_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE wines SET name=?, region=?, vintage=? WHERE wine_id=?', 
                  (wine.name, wine.region, wine.vintage, wine_id))
    conn.commit()
    conn.close()
    return {"message": f"Wine {wine_id} updated successfully"}

@app.delete("/wines/{wine_id}")
def delete_wine(wine_id: int):
    conn = get_sql_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM wines WHERE wine_id=?', (wine_id,))
    conn.commit()
    conn.close()
    return {"message": f"Wine {wine_id} deleted successfully"}

# CRUD Operations - MongoDB
@app.post("/mongo/wines/")
def create_mongo_wine(wine: WineCreate):
    wine_doc = {
        "name": wine.name,
        "region": wine.region,
        "vintage": wine.vintage,
        "created_date": datetime.now()
    }
    result = mongo_db.wines.insert_one(wine_doc)
    return {"message": "Wine created in MongoDB", "inserted_id": str(result.inserted_id)}

@app.get("/mongo/wines/")
def read_mongo_wines():
    wines = list(mongo_db.wines.find())
    for wine in wines:
        wine['_id'] = str(wine['_id'])
    return wines

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
