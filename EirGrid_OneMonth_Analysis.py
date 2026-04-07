import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta



# response extraction
def fetchAPIResponse(url, params):
    try:
        response = requests.get(url, params=params, timeout=10)

        if response.status_code != 200:
            print(f"{url} failed with status {response.status_code}")
            return []

        data = response.json()

        if "Rows" not in data:
            print(f"{url} unexpected format")
            return []

        return data.get("Rows", [])

    except Exception as e:
        print(f"{url} error:", e)
        return []



# #  https://www.smartgriddashboard.com/api/chart/?region=ROI&chartType=default&dateRange=day&dateFrom=30-Mar-2026&dateTo=30-Mar-2026&areas=solaractual,windactual,demandactual
# API 1 (wind, solar and demand)
def fetchapiResponse_1(date):
    url = "https://www.smartgriddashboard.com/api/chart/"
    params = {
        "region": "ROI",
        "chartType": "default",
        "dateRange": "day",
        "dateFrom": date,
        "dateTo": date,
        "areas": "solaractual,windactual"
    }
    return fetchAPIResponse(url, params)

# # https://www.smartgriddashboard.com/api/chart/?region=ROI&chartType=demand&dateRange=day&dateFrom=29-Mar-2026&dateTo=29-Mar-2026&areas=demandactual,demandforecast
# API 2 (actual demand)
def fetchapiResponse_2(date):
    url = "https://www.smartgriddashboard.com/api/chart/"
    params = {
        "region": "ROI",
        "chartType": "demand",
        "dateRange": "day",
        "dateFrom": date,
        "dateTo": date,
        "areas":"demandactual"
    }
    return fetchAPIResponse(url, params)

# https://www.smartgriddashboard.com/api/chart/?region=ROI&chartType=interconnection&dateRange=day&dateFrom=01-Apr-2026&dateTo=01-Apr-2026&areas=interconnection
# API 3 (interconnection)
def fetchapiResponse_3(date):
    url = "https://www.smartgriddashboard.com/api/chart/"
    params = {
        "region": "ROI",
        "chartType": "interconnection",
        "dateRange": "day",
        "dateFrom": date,
        "dateTo": date,
        "areas": "interconnection"
    }
    return fetchAPIResponse(url, params)



# Process the data from api response
def processData(rows):
    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame()

    if "EffectiveTime" not in df.columns:
        return pd.DataFrame()

    df["EffectiveTime"] = pd.to_datetime(df["EffectiveTime"])

    df = df.pivot_table(
        index="EffectiveTime",
        columns="FieldName",
        values="Value"
    ).reset_index()

    return df



# Merge all 3 dataframes to a single one
def mergeData(df1, df2, df3):

    df = pd.merge(df1, df2, on="EffectiveTime", how="outer")
    df = pd.merge(df, df3, on="EffectiveTime", how="outer")

    # Ensure columns exist
    for col in ["WIND_ACTUAL", "SOLAR_ACTUAL", "SYSTEM_DEMAND", "INTERCONNECTION"]:
        if col not in df.columns:
            df[col] = 0

    df.rename(columns={
        "EffectiveTime": "time",
        "WIND_ACTUAL": "wind",
        "SOLAR_ACTUAL": "solar",
        "SYSTEM_DEMAND": "actual_demand",
        "INTERCONNECTION": "interconnection"
    }, inplace=True)

    return df


# Feature Engineering task for data set preparation
def featureEngineering(df):

    df["hour"] = df["time"].dt.hour

    demand_safe = df["actual_demand"].replace(0, pd.NA)

    df["renewableRatio"] = (df["wind"] + df["solar"]) / demand_safe

    def renewable_Energy_category(x):
        if pd.isna(x):
            return "Unknown"
        elif x > 0.6:
            return "High Renewable"
        elif x > 0.3:
            return "Medium Renewable"
        else:
            return "Low Renewable"

    df["energyStatus"] = df["renewableRatio"].apply(renewable_Energy_category)

    def interconnection_category(x):
        if pd.isna(x):
            return "Unknown"
        elif x > 0:
            return "Import"
        elif x < 0:
            return "Export"
        else:
            return "Neutral"

    df["interconnectionStatus"] = df["interconnection"].apply(interconnection_category)

    df["importDependency"] = df["interconnection"] / demand_safe

    def sustainability(row):
        if row["interconnectionStatus"] == "Import" and row["energyStatus"] == "Low Renewable":
            return "High Carbon Risk"
        elif row["interconnectionStatus"] == "Export" and row["energyStatus"] == "High Renewable":
            return "Green Export"
        else:
            return "Balanced"

    df["sustainabilityStatus"] = df.apply(sustainability, axis=1)

    return df


# Date Generation
def generate_dates(start, end):
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%d-%b-%Y"))
        current += timedelta(days=1)

    return dates



# Save data to csv and DB
def saveData(df):
    df.to_csv("eirgrid_monthly_dataset.csv", index=False)
    engine = create_engine("sqlite:///eirGrid_Monthly.db")
    df.to_sql("eirgrid_monthly_data", engine, if_exists="replace", index=False)
    print("Saved Data Set as CSV and in DB")



# Fetch date for the last 30 days
def run_pipeline():

    # find last day
    endDate = datetime.now() - timedelta(days=1)
    # Start date, 30 day's back date
    startDate = endDate - timedelta(days=29)
    dates = generate_dates(startDate, endDate)
    print(f"Fetching from {startDate.date()} to {endDate.date()}")

    allData = []
    for date in dates:
        print(f"\n Fetch Response for: {date}")

        # Fetch each api
        api1 = fetchapiResponse_1(date)
        api2 = fetchapiResponse_2(date)
        api3 = fetchapiResponse_3(date)
        print("No of Rowsin API1:", len(api1))
        print("No of Rows in API2:", len(api2))
        print("No of Rows in API3:", len(api3))
        if len(api1) == 0 or len(api2) == 0 or len(api3) == 0:
            print(f"Skipping {date} (incomplete API data)")
            continue
        # Processing api responses
        df1 = processData(api1)
        df2 = processData(api2)
        df3 = processData(api3)

        if df1.empty and df2.empty and df3.empty:
           print(f"Skipping {date} (all APIs empty)")
           continue

        df = mergeData(df1, df2, df3)
        
        # Take the last quarter's value from an hour as the value for each hr
        numeric_cols = df.select_dtypes(include="number").columns
        df = (
            df.set_index("time")[numeric_cols]
            .resample("1h")
            .last()
            .reset_index()
        )
        
        df = featureEngineering(df)
        df["date"] = date
        allData.append(df)
    df = pd.concat(allData, ignore_index=True)
    print("\nRows per day:")
    print(df.groupby("date").size())
    saveData(df)


# Run program
if __name__ == "__main__":
    run_pipeline()