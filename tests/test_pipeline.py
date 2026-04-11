import pandas as pd
from utils import transform_data

# Unit for the pipeline

# Test1, verify sustainability exist or not
def test_sustainability_exists():
    # create sample data for trsting
    df = pd.DataFrame({
        "renewable_percentage": [50],
        "interconnection": [20],
        "actual_demand": [100],
        "co2emission": [200]
    })

    # call function transform_data defined in utils class
    result = transform_data(df)

    # Verify the resuly
    assert "sustainability" in result.columns

# Test2 , check the sustainability range of eirgird    
def test_sustainability_range():
    df = pd.DataFrame({
        "renewable_percentage": [50],
        "interconnection": [20],
        "actual_demand": [100],
        "co2emission": [200]
    })

    result = transform_data(df)

    value = result["sustainability"].iloc[0]

    assert value >= 0
    assert value <= 100   
    
# Test 3, check import percentatge caculation
def test_import_percentage():
    df = pd.DataFrame({
        "renewable_percentage": [40],
        "interconnection": [10],
        "actual_demand": [100],
        "co2emission": [100]
    })

    result = transform_data(df)
    # Get the first row value of import_pct
    assert result["import_pct"].iloc[0] == 10.0 

# Test 4, check for dataframe is empty or not    
def test_handles_empty_dataframe():
    import pandas as pd
    from utils import transform_data

    df = pd.DataFrame(columns=[
        "renewable_percentage",
        "interconnection",
        "actual_demand",
        "co2emission"
    ])

    result = transform_data(df)

    assert isinstance(result, pd.DataFrame)        