import pandas as pd
from cassandra.cluster import Cluster
from datetime import datetime as dt
from uuid import uuid4
from types import FunctionType
import logging

def convert_date(*args, **kwargs):
    date_obj = args[0]
    return dt.strptime(str(date_obj), "%Y-%m-%d").strftime("%d-%b-%y")

def extract_and_transform_bronze_turbine_data(*args, **kwargs) -> None:
    
    session = args[0]
    
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
    
    try:
        rows = session.execute(select_query)
        data = [row._asdict() for row in rows]

        # Convert to DataFrame
        df = pd.DataFrame(data)
        df["recorded_date"] = df["recorded_date"].map(lambda x : convert_date(x))
    
        for i in range(len(df)):
            session.execute(insert_query, tuple([uuid4(), *df.iloc[i]]))
    except Exception as e:
        logging.error(str(e))
        
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
            recorded_date,
            window,
            winddirection
        FROM
            iotsolution.bronze_weather_sensor
    """
    
    insert_to_silver = """
        INSERT INTO iotsolution.silver_aggregate_weather (id, device_id, recorded_date, humidity, temperature, windspeed, winddirection)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        rows_1 = session.execute(select_query)
        
        # Convert to DataFrame
        df = pd.DataFrame([row._asdict() for row in rows_1])
        df["recorded_date"] = df["recorded_date"].map(lambda x : convert_date(x))
        
        rows_2 = session.execute(latest_winddirection)
        
        winddirection_df = pd.DataFrame([row._asdict() for row in rows_2])
        winddirection_df["recorded_date"] = winddirection_df["recorded_date"].map(lambda x : convert_date(x))
        
        latest_winddirection = winddirection_df.loc[winddirection_df.groupby(['device_id', 'recorded_date'])['window'].idxmax()]
        
        result_df = pd.merge(df, latest_winddirection[['device_id', 'recorded_date', 'winddirection']], on=['device_id', 'recorded_date'], how='left')
    
        for i in range(len(result_df)):
            session.execute(insert_to_silver, tuple([uuid4(), *result_df.iloc[i]]))
            
    except Exception as e:
        logging.error(str(e))
        
if __name__ == "__main__":
    
    functions_to_call = [extract_and_transform_bronze_turbine_data, extract_and_transform_bronze_weather_data]

    with Cluster(['127.0.0.1']).connect() as session:
        for func in functions_to_call:
            if isinstance(func, FunctionType):
                func(session)