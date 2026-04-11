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


#  https://www.smartgriddashboard.com/api/chart/?region=ROI&chartType=default&dateRange=day&dateFrom=30-Mar-2026&dateTo=30-Mar-2026&areas=solaractual,windactual,demandactual
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
    return fetchAPIResponse(url, params=params)


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
    
    return fetchAPIResponse(url, params=params)

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
    return fetchAPIResponse(url, params=params)

# Get API Response
def fetchAPIResponse(url, params):
    try:
        response = requests.get(url, params=params, timeout=10)
        # Check request status
        if response.status_code != 200:
            print(f"{url} failed with status {response.status_code}")
            return []

        data = response.json()
        # Check expected structure
        if "Rows" not in data:
            print(f"{url} returned unexpected format")
            return []
        return data.get("Rows", [])

    except requests.exceptions.Timeout:
        print(f"{url} timeout error")
    except requests.exceptions.ConnectionError:
        print(f"{url} connection error")
    except Exception as e:
        print(f"{url} unexpected error:", e)



# Transform data
def processData(rows):
    df = pd.DataFrame(rows)
    if df.empty:
        print("No data received from API")
        return pd.DataFrame()
    
    if "EffectiveTime" not in df.columns:
        print("EffectiveTime column missing. Columns received:", df.columns)
        return pd.DataFrame()
    
    # Convert time
    df["EffectiveTime"] = pd.to_datetime(df["EffectiveTime"])

    # Pivot table
    df = df.pivot_table(
        index="EffectiveTime",
        columns="FieldName",
        values="Value"
    ).reset_index()

    return df


# Merge
def mergeData(df1, df2, df3):
    print(f"Data Frame 1: \n {df1.head(5)}")
    print(f"Data Frame 2: \n {df2.head(5)}")
    print(f"Data Frame 3: \n {df3.head(5)}")
    
    # merge df1 and df2
    df = pd.merge(df1, df2, on="EffectiveTime", how="outer")

    # now merge current df with df3
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
    
    for col in ["wind", "solar", "actual_demand", "interconnection"]:
        if col not in df.columns:
            df[col] = 0
            
    print(f"Final Data Set Columns: \n {df.head(5)}")
    return df

# Feature Engineering
def featureEngineering(df):

    # Time feature
    df["hour"] = df["time"].dt.hour
    
    # Avoid division by zero
    demand_safe = df["actual_demand"].replace(0, pd.NA)
    
    # Calculate Renewable Energy  Ratio
    #df["renewableRatio"] = (df["wind"] + df["solar"]) / (df["actual_demand"].replace(0, pd.NA))
    df["renewableRatio"] = (df["wind"] + df["solar"]) / demand_safe
    
    # Renewable classification
    def find_renewable_category(x):
        if pd.isna(x):
            return "Unknown"
        elif x > 0.6:
            return "High Renewable"
        elif x > 0.3:
            return "Medium Renewable"
        else:
            return "Low Renewable"

    df["energyStatus"] = df["renewableRatio"].apply(find_renewable_category)

    # Energy Interconnection classification as import energy or export energy
    
    def find_interconnection_classification(x):
        if pd.isna(x):
            return "Unknown"
        elif x > 0:
            return "Import"
        elif x < 0:
            return "Export"
        else:
            return "Neutral"
    
    df["interconnection_status"] = df["interconnection"].apply(find_interconnection_classification)
    
    # Calculate the Import dependency level of Ireland
    df["import_dependency"] = df["interconnection"] / demand_safe


    # Calculating the Sustainability label of irelands Eirgrid
    def sustainability(row):
        if row["interconnection_status"] == "Import" and row["energy_status"] == "Low Renewable":
            return "High Carbon Risk"
        elif row["interconnection_status"] == "Export" and row["energy_status"] == "High Renewable":
            return "Green Export"
        else:
            return "Balanced"

    df["sustainability_status"] = df.apply(sustainability, axis=1)
    return df



# Save the dataset creatred to
def save_data(df, date):
    filename = f"eirgrid_{date}.csv"
    df.to_csv(filename, index=False)

    engine = create_engine("sqlite:///energyEirGrid.db")
    df.to_sql("eirgrid_data", engine, if_exists="append", index=False)

    print(f"Saved CSV + DB for {date}")


# Main Pipeline
def run_pipeline():
    today = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y")
    print(f"Running pipeline for {today}")

    try:
        # Fetch all APIs
        api1 = fetchapiResponse_1(today)
        print("ap1 response")
        api2 = fetchapiResponse_2(today)
        print("ap2 response")
        api3 = fetchapiResponse_3(today)
        print("ap3 response")

        # Process all data frames 
        df1 = processData(api1)
        df2 = processData(api2)
        df3 = processData(api3)

        # Merge
        df = mergeData(df1, df2, df3)

        # Feature Engineering
        df = featureEngineering(df)

        # Remove duplicates
        df.drop_duplicates(subset=["time"], inplace=True)

        print(df.head())

        # Save data
        save_data(df, today)

    except Exception as e:
        print("Error:", e)

# Run Programe 
if __name__ == "__main__":
    run_pipeline()