from flask import Flask, render_template, jsonify, request
import pandas as pd
import sqlite3

app = Flask(__name__)

DB_PATH = "energyEirGridModified1.db"


def get_data(date=None):
    conn = sqlite3.connect(DB_PATH)

    query = "SELECT * FROM energyEirGridModified_data1"
    df = pd.read_sql(query, conn)

    conn.close()
    df[["wind","solar","actual_demand","interconnection","co2emission"]] = \
        df[["wind","solar","actual_demand","interconnection","co2emission"]] \
        .apply(pd.to_numeric, errors="coerce")

    # Calculations
    demand_safe = df["actual_demand"].replace(0, pd.NA)
    df["import_pct"] = (df["interconnection"] / demand_safe) * 100
    max_co2 = df["co2emission"].max()

    if max_co2 != 0:
        df["co2_norm"] = df["co2emission"] / max_co2
    else:
        df["co2_norm"] = 0

    # NEW sustainability calculation (normalized)
    df["sustainability_raw"] = (
        0.6 * df["renewable_percentage"]
        - 0.2 * df["import_pct"]
        - 0.2 * (df["co2_norm"] * 100)
    )

    min_val = df["sustainability_raw"].min()
    max_val = df["sustainability_raw"].max()

    # Avoid division 0 division error
    if max_val != min_val:
        df["sustainability"] = (
            (df["sustainability_raw"] - min_val) / (max_val - min_val)
        ) * 100
    else:
        df["sustainability"] = 50

    # Time formatting
    df["time"] = pd.to_datetime(df["time"])

    # Filter by date (if provided)
    if date:
        df = df[df["time"].dt.strftime("%Y-%m-%d") == date]

    # Sort
    df = df.sort_values("time")

    return df


# html template route
@app.route("/")
def index():
    return render_template("pipeline.html")


# data fetch API
@app.route("/data")
def data():
    date = request.args.get("date")
    df = get_data(date)

    if not df.empty and "sustainability" in df.columns:
        latest_val = df["sustainability"].dropna()
        latest = float(latest_val.iloc[-1]) if not latest_val.empty else 0
    else: latest = 0    

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


# Separate sustainability endpoint
@app.route("/sustainability")
def sustainability():
    date = request.args.get("date")
    df = get_data(date)

    return jsonify({
        "time": df["time"].dt.strftime("%H:%M").tolist(),
        "sustainability": df["sustainability"].fillna(0).tolist()
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8888, debug=True)