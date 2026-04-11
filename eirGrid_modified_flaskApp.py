from flask import Flask, render_template, jsonify, request
import pandas as pd
import sqlite3

app = Flask(__name__)

def get_data(date=None):
    conn = sqlite3.connect("energyEirGridModified.db")

    query = "SELECT * FROM energyEirGridModified_data"
    df = pd.read_sql(query, conn)

    conn.close()

    df["time"] = pd.to_datetime(df["time"])

    if date:
        df = df[df["time"].dt.strftime("%Y-%m-%d") == date]

    df = df.sort_values("time")

    return df


@app.route("/")
def index():
    return render_template("pipeline.html")


@app.route("/data")
def data():
    date = request.args.get("date")
    df = get_data(date)

    return jsonify({
        "time": df["time"].dt.strftime("%H:%M").tolist(),
        "wind": df["wind"].tolist(),
        "solar": df["solar"].tolist(),
        "co2": df["co2emission"].tolist(),
        "renewable": df["renewable_percentage"].fillna(0).tolist(),
        "interconnection": df["interconnection"].tolist()
    })


if __name__ == "__main__":
    app.run(debug=True)