from unittest import result
import pandas as pd
from cassandra.cluster import Cluster
from datetime import datetime as dt
from uuid import uuid4
from types import FunctionType
import logging

def extract_and_transform_data_to_gold(*args, **kwargs):
    session = args[0]
    
    silver_turbine = """
        SELECT * FROM iotsolution.silver_aggregate_turbine
    """
    
    silver_weather = """
        SELECT * FROM iotsolution.silver_aggregate_weather
    """
    
    insert_to_gold = """
        INSERT INTO iotsolution.gold_enriched_turbine (id, device_id, recorded_date, angle, rpm, humidity, temperature, winddirection, windspeed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        turbine_df = pd.DataFrame([row._asdict() for row in session.execute(silver_turbine)])
        weather_df = pd.DataFrame([row._asdict() for row in session.execute(silver_weather)])
        
        result_df = pd.merge(turbine_df, weather_df[['device_id', 'recorded_date', 'humidity', 'temperature', 'winddirection', 'windspeed']], on=['device_id', 'recorded_date'], how='inner')
        result_df.drop(columns=['id'], inplace=True)

        for i in range(len(result_df)):
            session.execute(insert_to_gold, tuple([uuid4(), *result_df.iloc[i]]))
    
    except Exception as e:
        logging.error(str(e))
        return result_df

if __name__ == "__main__":
    
    with Cluster(['127.0.0.1']).connect() as session:
        extract_and_transform_data_to_gold(session)