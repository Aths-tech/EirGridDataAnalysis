from flask import Flask, render_template, jsonify, request
import pandas as pd
import sqlite3

app = Flask(__name__)

DB_PATH = "energyEirGridModified.db"


def get_data(date=None):
    conn = sqlite3.connect(DB_PATH)

    query = "SELECT * FROM energyEirGridModified_data"
    df = pd.read_sql(query, conn)

    conn.close()

    # 🔹 Calculations
    df["import_pct"] = (df["interconnection"] / df["actual_demand"]) * 100
    df["co2_norm"] = df["co2emission"] / df["co2emission"].max()

    # 🔹 Calculations
    df["import_pct"] = (df["interconnection"] / df["actual_demand"]) * 100
    df["co2_norm"] = df["co2emission"] / df["co2emission"].max()

    # 🔹 NEW sustainability calculation (normalized)
    df["sustainability_raw"] = (
        0.6 * df["renewable_percentage"]
        - 0.2 * df["import_pct"]
        - 0.2 * (df["co2_norm"] * 100)
    )

    min_val = df["sustainability_raw"].min()
    max_val = df["sustainability_raw"].max()

    df["sustainability"] = (
        (df["sustainability_raw"] - min_val) / (max_val - min_val)
    ) * 100

    # 🔹 Time formatting
    df["time"] = pd.to_datetime(df["time"])

    # 🔹 Filter by date (if provided)
    if date:
        df = df[df["time"].dt.strftime("%Y-%m-%d") == date]

    # 🔹 Sort
    df = df.sort_values("time")

    return df


# 🔹 Home route
@app.route("/")
def index():
    return render_template("pipeline.html")


# 🔹 Main data API
@app.route("/data")
def data():
    date = request.args.get("date")
    df = get_data(date)

    latest = df["sustainability"].iloc[-1]

    return jsonify({
        "time": df["time"].dt.strftime("%H:%M").tolist(),
        "wind": df["wind"].fillna(0).tolist(),
        "solar": df["solar"].fillna(0).tolist(),
        "co2": df["co2emission"].fillna(0).tolist(),
        "renewable": df["renewable_percentage"].fillna(0).tolist(),
        "interconnection": df["interconnection"].fillna(0).tolist(),
        "sustainability": df["sustainability"].fillna(0).tolist(),
        "latest_sustainability": float(latest)   # ✅ ADD THIS
    })


# 🔹 Separate sustainability endpoint (optional)
@app.route("/sustainability")
def sustainability():
    date = request.args.get("date")
    df = get_data(date)

    return jsonify({
        "time": df["time"].dt.strftime("%H:%M").tolist(),
        "sustainability": df["sustainability"].fillna(0).tolist()
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)