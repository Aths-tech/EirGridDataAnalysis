import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

'''
This program will fetch data from EirGrid site apis to fetch :
1. Solar, wind contribution to energy generation
2. The actual demand hourly, here we are taking the last quarter of the hour data
3. Interconnection shows the flow of energy between Ireland and Wales,Northern Ireland and Scotland.Flows from Great Britain to Ireland are shown as a positive MW transfer, while those from Ireland to Great Britain are shown as a negative MW transfer. 

This program will fetch the hourly data (last quarter value of hr) for last 30 days, from above 3 apis, get the details, create data set features from each api response
Calculate the   
'''

# API 1 (wind, solar)
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
    return fetchAPIResponse(url, params=params)


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
    return fetchAPIResponse(url, params=params)


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
    return fetchAPIResponse(url, params=params)


# API 4 (co2 emission)
def fetchapiResponse_4(date):
    url = "https://www.smartgriddashboard.com/api/chart/"
    params = {
        "region": "ROI",
        "chartType": "co2",
        "dateRange": "day",
        "dateFrom": date,
        "dateTo": date,
        "areas": "co2emission"
    }
    return fetchAPIResponse(url, params=params)


# Get API Response
def fetchAPIResponse(url, params):
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code != 200:
            print(f"{url} failed with status {response.status_code}")
            return []

        data = response.json()
        if "Rows" not in data:
            print(f"{url} returned unexpected format")
            return []
        return data.get("Rows", [])

    except Exception as e:
        print(f"{url} error:", e)
        return []


# Transform data
def processData(rows):
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame()

    df["EffectiveTime"] = pd.to_datetime(df["EffectiveTime"])

    df = df.pivot_table(
        index="EffectiveTime",
        columns="FieldName",
        values="Value",
        aggfunc="first"
    ).reset_index()

    return df


# Merge
def mergeData(df_demand, df_interconnection, df_wind_solar, df_co2emission):

    df = pd.merge(df_demand, df_interconnection, on="EffectiveTime", how="outer")
    df = pd.merge(df, df_wind_solar, on="EffectiveTime", how="outer")
    df = pd.merge(df, df_co2emission, on="EffectiveTime", how="outer")

    # Rename correct API fields
    df.rename(columns={
        "EffectiveTime": "time",
        "SYSTEM_DEMAND": "actual_demand",
        "WIND_ACTUAL": "wind",
        "SOLAR_ACTUAL": "solar",
        "INTER_NET_ROI": "interconnection",
        "CO2_EMISSIONS": "co2emission"
    }, inplace=True)
    df.drop(columns=["INTER_EWIC", "INTER_GRNLK"], errors="ignore", inplace=True)
    # Ensure required columns exist
    required_cols = ["wind", "solar", "actual_demand", "interconnection", "co2emission"]
    for col in required_cols:
        if col not in df.columns:
            print(f" Column: {col} missing, filling with 0")
            df[col] = 0

    return df


# Feature Engineering
def featureEngineering(df):

    # Find Renewable energy contribution(wind + solar)
    df["renewable_contribution"] = df["wind"] + df["solar"]

    # Renewable contribution %
    df["renewable_percentage"] = (
        df["renewable_contribution"] / df["actual_demand"].replace(0, pd.NA)
    ) * 100

    df["renewable_percentage"] = df["renewable_percentage"].clip(upper=100)

    # Interconnection classification
    def interCon_status(x):
        if pd.isna(x):
            return "Unknown"
        elif x > 0:
            return "Import"
        elif x < 0:
            return "Export"
        else:
            return "Neutral"

    df["interconnection_status"] = df["interconnection"].apply(interCon_status)

    return df


# Save data
def save_data(df, date):
    filename = f"energyEirGridModified_{date}.csv"
    df.to_csv(filename, index=False)

    engine = create_engine("sqlite:///energyEirGridModified1.db")
    df.to_sql("energyEirGridModified_data1", engine, if_exists="append", index=False)

    print(f"Saved CSV + DB for {date}")


# generateDate
def get_previous_day():
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%d-%b-%Y")


# Main Pipeline
def run_pipeline():
    try:
        date = get_previous_day()
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"Fetching data for: {date}")
        
        # Get All 4 api responses
        solar_wind_response = fetchapiResponse_1(date)
        demand_response = fetchapiResponse_2(date)
        intercon_response = fetchapiResponse_3(date)
        co2emission = fetchapiResponse_4(date)

        # Create Dataframe from the api response
        df_wind_solar = processData(solar_wind_response)
        df_demand = processData(demand_response)
        df_interconnection = processData(intercon_response)
        df_co2emission = processData(co2emission)

        # Use INTER_NET_ROI only
        if "INTER_NET_ROI" in df_interconnection.columns:
            df_interconnection["INTER_NET_ROI"] = df_interconnection["INTER_NET_ROI"]
        else:
            df_interconnection["INTER_NET_ROI"] = 0

        # Merge all the dataframes created as a single dataset
        df = mergeData(df_demand, df_interconnection, df_wind_solar, df_co2emission)

        # Feature Engineering
        df = featureEngineering(df)
              
        df.fillna(0, inplace=True)
        df.drop_duplicates(subset=["time"], inplace=True)
        df = df.sort_values("time")

        print(df.head())

        save_data(df, today)

    except Exception as e:
        print("Error:", e)


# Run Pipeline
if __name__ == "__main__":
    run_pipeline()
