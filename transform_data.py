import pandas as pd
from cassandra.cluster import Cluster
from datetime import datetime as dt
from prepare_data import insert_data
from uuid import uuid4

def convert_date(*args, **kwargs):
    date_obj = args[0]
    return dt.strptime(str(date_obj), "%Y-%m-%d").strftime("%d-%b-%y")

def extract_and_transform_bronze_turbine_data(*args, **kwargs) -> None:
    
    session = args[0]
    device_id = args[1]
    
    select_query = f"""
        SELECT 
            DEVICE_ID, 
            RECORDED_DATE, 
            AVG(ANGLE) AS ANGLE, 
            AVG(RPM) AS RPM 
        FROM 
            iotsolution.BRONZE_TURBINE_SENSOR
        GROUP BY 
            DEVICE_ID, RECORDED_DATE
    """
    
    insert_query = """
        INSERT INTO iotsolution.silver_aggregate_turbine (id, device_id, recorded_date, angle, rpm)
        VALUES (%s, %s, %s, %s, %s)
    """
    
    rows = session.execute(select_query)
    data = [row._asdict() for row in rows]

    # Convert to DataFrame
    df = pd.DataFrame(data)
    df["recorded_date"] = df["recorded_date"].map(lambda x : convert_date(x))
    
    for i in range(len(df)):
        session.execute(insert_query, tuple([uuid4(), *df.iloc[i]]))
        
def extract_and_transform_bronze_weather_data(*args, **kwargs) -> None:
    session = args[0]
    
    select_query = """
        SELECT
            device_id,
            recorded_date,
            AVG(humidity) AS humidity,
            AVG(temperature) AS temperature,
            AVG(windspeed) AS windspeed
        FROM
            iotsolution.bronze_weather_sensor
        GROUP BY
            device_id, recorded_date
    """
    
    latest_winddirection = """
        SELECT
            device_id,
            window,
            winddirection
        FROM
            iotsolution.bronze_weather_sensor
    """
    
    rows = session.execute(select_query)
    data = [row._asdict() for row in rows]
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    df["recorded_date"] = df["recorded_date"].map(lambda x : convert_date(x))
    
    rows = session.execute(latest_winddirection)
    data = [row._asdict() for row in rows]
    
    winddirection_df = pd.DataFrame(data)
    
    return winddirection_df
        
if __name__ == "__main__":
    
    with Cluster(['127.0.0.1']).connect('iotsolution') as session:
        extract_and_transform_bronze_turbine_data(session)
    