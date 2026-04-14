import pandas as pd

def transform_Data(df):
    df["import_pct"] = (df["interconnection"] / df["actual_demand"]) * 100
    df["co2_norm"] = df["co2emission"] / df["co2emission"].max()

    df["sustainability_raw"] = (
        0.6 * df["renewable_percentage"]
        - 0.2 * df["import_pct"]
        - 0.2 * (df["co2_norm"] * 100)
    )

    min_val = df["sustainability_raw"].min()
    max_val = df["sustainability_raw"].max()

    if max_val != min_val:
        df["sustainability"] = (
            (df["sustainability_raw"] - min_val) / (max_val - min_val)
        ) * 100
    else:
        df["sustainability"] = 0

    return df