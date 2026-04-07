from flask import Flask, render_template, request
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import os

app = Flask(__name__)

engine = create_engine("sqlite:///eirGrid_Monthly.db")


@app.route("/", methods=["GET", "POST"])
def index():


    # Fetch Data From DB
    df = pd.read_sql("SELECT * FROM eirgrid_monthly_data", engine)

    if df.empty:
        return "No data available"

    df["time"] = pd.to_datetime(df["time"])

    # Get Available Dates
    available_dates = sorted(df["date"].unique())

    selected_date = request.form.get("date") if request.method == "POST" else available_dates[-1]

    # Filter data for selected date
    df_day = df[df["date"] == selected_date]


    # Find missing dates
    all_dates = pd.date_range(
        start=pd.to_datetime(min(available_dates), format="%d-%b-%Y"),
        end=pd.to_datetime(max(available_dates), format="%d-%b-%Y")
    )

    all_dates_str = [d.strftime("%d-%b-%Y") for d in all_dates]

    missing_dates = list(set(all_dates_str) - set(available_dates))

    # Create Static folder
    if not os.path.exists("static"):
        os.makedirs("static")

    # Graph for Wind vs Demand (selected day)
    plt.figure()
    plt.plot(df_day["time"], df_day["wind"], label="Wind")
    plt.plot(df_day["time"], df_day["actual_demand"], label="Demand")
    plt.legend()
    plt.xticks(rotation=45)

    wind_plot = "static/wind_vs_demand.png"
    plt.savefig(wind_plot)
    plt.close()

    # Graph for Renewable Ratio (selected day)
    plt.figure()
    plt.plot(df_day["time"], df_day["renewableRatio"])
    plt.xticks(rotation=45)

    ratio_plot = "static/renewable_ratio.png"
    plt.savefig(ratio_plot)
    plt.close()

    # Graph for renewvable energy trend (avg renewable ratio)
    df_trend = df.groupby("date")["renewableRatio"].mean().reset_index()

    plt.figure()
    plt.plot(df_trend["date"], df_trend["renewableRatio"])
    plt.xticks(rotation=45)

    trend_plot = "static/trend.png"
    plt.savefig(trend_plot)
    plt.close()

    # Visualize
    return render_template(
        "index1.html",
        table=df_day.to_html(index=False),
        dates=available_dates,
        selected_date=selected_date,
        missing_dates=missing_dates,
        wind_plot=wind_plot,
        ratio_plot=ratio_plot,
        trend_plot=trend_plot
    )


if __name__ == "__main__":
    app.run(debug=True) 