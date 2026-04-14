import pandas as pd
from utils import transform_Data

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

    # call function transform_Data defined in utils class
    result = transform_Data(df)

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

    result = transform_Data(df)

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

    result = transform_Data(df)
    # Get the first row value of import_pct
    assert result["import_pct"].iloc[0] == 10.0 

# Test 4, check for dataframe is empty or not    
def test_handles_empty_dataframe():
    import pandas as pd
    from utils import transform_Data

    df = pd.DataFrame(columns=[
        "renewable_percentage",
        "interconnection",
        "actual_demand",
        "co2emission"
    ])

    result = transform_Data(df)

    assert isinstance(result, pd.DataFrame) 
    
# Test 5, verify that CO₂ normalization step is created correctly
def test_co2_normalization():
    df = pd.DataFrame({
        "renewable_percentage": [50],
        "interconnection": [10],
        "actual_demand": [100],
        "co2emission": [100]
    })

    result = transform_Data(df)

    assert "co2_norm" in result.columns  

# Test 6, verify no division by 0 error
def test_no_division_by_zero():
    df = pd.DataFrame({
        "renewable_percentage": [50],
        "interconnection": [10],
        "actual_demand": [0],  # edge case
        "co2emission": [100]
    })

    result = transform_Data(df)

    assert not result["import_pct"].isnull().all()             