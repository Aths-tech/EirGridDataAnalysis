from flask import Flask, render_template, request
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import os

app = Flask(__name__)

engine = create_engine("sqlite:///eirGrid_Monthly.db")


@app.route("/", methods=["GET", "POST"])
def index():

    selected_date = None

    if request.method == "POST":
        selected_date = request.form.get("date")

        query = f"""
        SELECT * FROM eirgrid__monthly_data
        WHERE date = '{selected_date}'
        ORDER BY time
        """
    else:
        query = """
        SELECT * FROM eirgrid__monthly_data
        ORDER BY time DESC
        LIMIT 100
        """

    df = pd.read_sql(query, engine)

    if df.empty:
        return "No data available"

    df["time"] = pd.to_datetime(df["time"])

    # Ensure static folder
    if not os.path.exists("static"):
        os.makedirs("static")

    # Wind vs Demand Graph
    plt.figure()
    plt.plot(df["time"], df["wind"], label="Wind")
    plt.plot(df["time"], df["actual_demand"], label="Demand")
    plt.legend()
    plt.xticks(rotation=45)

    wind_plot = "static/wind_vs_demand.png"
    plt.savefig(wind_plot)
    plt.close()

    # Renewable Ratio Graph
    plt.figure()
    plt.plot(df["time"], df["renewableRatio"])
    plt.xticks(rotation=45)

    ratio_plot = "static/renewable_ratio.png"
    plt.savefig(ratio_plot)
    plt.close()

   
    # Interconnection Graph
    plt.figure()
    plt.plot(df["time"], df["interconnection"])
    plt.xticks(rotation=45)

    inter_plot = "static/interconnection.png"
    plt.savefig(inter_plot)
    plt.close()

    # Get unique dates for dropdown
    dates = pd.read_sql("SELECT DISTINCT date FROM eirgrid__monthly_data", engine)

    return render_template(
        "index1.html",
        table=df.to_html(index=False),
        wind_plot=wind_plot,
        ratio_plot=ratio_plot,
        inter_plot=inter_plot,
        dates=dates["date"].tolist(),
        selected_date=selected_date
    )


if __name__ == "__main__":
    app.run(debug=True)