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
    # Fetch first 10 rows from DB
    conn = sqlite3.connect("energyEirGridModified1.db")
    cursor = conn.cursor()

    cursor.execute("""
    SELECT wind FROM energyEirGridModified_data1 
    ORDER BY time LIMIT 10
""")
    db_rows = cursor.fetchall()

    conn.close()

    # Convert DB fetched result to list list
    db_wind = [row[0] for row in db_rows]

    # Call Flask API and fetch data
    client = app.test_client()
    response = client.get("/data")

    assert response.status_code == 200

    data = response.get_json()

    api_wind = data["wind"][:10]  # first 10 values

    # Compare responses
    for db_val, api_val in zip(db_wind, api_wind):
        assert round(db_val, 2) == round(api_val, 2)  