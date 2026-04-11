from eirGrid_modified_flaskApp import app
import sqlite3

# sample integration test for the pipeline 
# Verify Flask api, Database and the Transformation logic for data is working fine
def test_data_endpoint():
    client = app.test_client()

    response = client.get("/data")

    assert response.status_code == 200

    data = response.get_json()

    assert "time" in data
    assert "sustainability" in data
    assert len(data["time"]) > 0
    
def test_api_matches_db():
    # 🔹 Step 1: Fetch first 10 rows directly from DB
    conn = sqlite3.connect("energyEirGridModified.db")
    cursor = conn.cursor()

    cursor.execute("SELECT wind FROM energyEirGridModified_data LIMIT 10")
    db_rows = cursor.fetchall()

    conn.close()

    # Convert DB result → list
    db_wind = [row[0] for row in db_rows]

    # 🔹 Step 2: Call Flask API
    client = app.test_client()
    response = client.get("/data")

    assert response.status_code == 200

    data = response.get_json()

    api_wind = data["wind"][:10]  # first 10 values

    # 🔹 Step 3: Compare
    assert db_wind == api_wind    