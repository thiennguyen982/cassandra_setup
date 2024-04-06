import pandas as pd
from cassandra.cluster import Cluster
from uuid import uuid4
from datetime import datetime as dt
import os

path = os.getcwd()
data_folder = path + "\data"

def preprocess_date(date_str):
    
    try:
        date_obj = dt.strptime(date_str, '%m/%d/%Y')
        return date_obj.strftime('%Y-%m-%d')
    
    except ValueError as ve:
        return str(date_str)
    
def create_table(*args, **kwargs) -> None:
    cql_file_path = args[0]
    session = args[1]
    
    with open(cql_file_path, 'r') as cql_file:
        cql_queries = cql_file.read()
        
        for query in cql_queries.split(";"):
            query = query.strip()
            if query:
                session.execute(query)
    
def insert_data(*args, **kwargs) -> None:
    insert_query = args[0]
    data_file = args[1]
    session = args[2]
    
    try:
        df = pd.DataFrame()
        df = pd.read_csv(data_folder + "\\" + data_file)
        df['date'] = df['date'].apply(preprocess_date)

        for j in range(len(df)):
            row = df.iloc[j]
            data = tuple([uuid4(), *row])
            session.execute(insert_query, data)
            
    except Exception as e:
        print(str(e))
        
if __name__ == "__main__":
    
    insert_query = [
        """
            INSERT INTO iotsolution.bronze_turbine_sensor (id, angle, device_id, recorded_date, rpm, window)
            VALUES (%s, %s, %s, %s, %s, %s)
        """,
        """
            INSERT INTO iotsolution.bronze_weather_sensor (id, device_id, humidity, recorded_date, temperature, winddirection, window, windspeed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        """
            INSERT INTO iotsolution.gold_enriched_turbine (id, device_id, angle, humidity, recorded_date, rpm, temperature, winddirection, windspeed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        """
            INSERT INTO iotsolution.silver_aggregate_turbine (id, angle, device_id, recorded_date, rpm)
            VALUES (%s, %s, %s, %s, %s)
        """,
        """
            INSERT INTO iotsolution.silver_aggregate_weather (id, device_id, humidity, recorded_date, temperature, winddirection, windspeed)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
    ]
    
    data_file = ["raw_turbine_data.csv", "raw_weather_sensor.csv", "sample_enriched_data.csv",
                 "sample_aggregate_turbine.csv", "sample_aggregate_weather.csv"]
    
    with Cluster(['127.0.0.1']).connect() as session:
        
        create_table("de_project.cql", session)
        
        for i in range(len(insert_query)):
            insert_data(insert_query[i], data_file[i], session)