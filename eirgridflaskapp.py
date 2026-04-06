from flask import Flask, render_template
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import os

app = Flask(__name__)

engine = create_engine("sqlite:///energy.db")


@app.route("/")
def index():
    query = """
    SELECT * FROM eirgrid_data
    ORDER BY time DESC
    LIMIT 50
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        return "No data available. Run pipeline first."

    df = df.sort_values("time")

    # Create graph
    plt.figure()
    plt.plot(df["time"], df["renewable_ratio"])
    plt.xticks(rotation=45)

    if not os.path.exists("static"):
        os.makedirs("static")

    plot_path = "static/renewable_plot.png"
    plt.savefig(plot_path)
    plt.close()

    return render_template(
        "index.html",
        table=df.to_html(index=False),
        plot_url=plot_path
    )


if __name__ == "__main__":
    app.run(debug=True)