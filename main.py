import pandas as pd
import os
from cassandra.cluster import Cluster
from uuid import uuid4
from datetime import datetime as dt

path = os.getcwd()
data_folder = path + "\data"

def preprocess_date(date_str):
    date_obj = dt.strptime(date_str, '%m/%d/%Y')
    return date_obj.strftime('%Y-%m-%d')

if __name__ == "__main__":
    
    insert_query = [
        """
            INSERT INTO iotsolution.bronze_turbine_sensor (id, angle, device_id, recorded_date, rpm, window)
            VALUES (%s, %s, %s, %s, %s, %s)
        """,
        """
            INSERT INTO iotsolution.bronze_weather_sensor (id, device_id, humidity, recorded_date, temperature, winddirection, window, windspeed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
    ]
    
    data_file = ["raw_turbine_data.csv", "raw_weather_sensor.csv"]
    
    with Cluster(['127.0.0.1']).connect('iotsolution') as session:

        with open("de_project.cql", 'r') as cql_file:
            cql_queries =  cql_file.read()

        for query in cql_queries.split(":"):
            query = query.strip()
            if query:
                session.execute(query)
    
        for i in range(len(insert_query)):
            try:
                df = pd.DataFrame()
                df = pd.read_csv(data_folder + "\\" + data_file[i])
                df['date'] = df['date'].apply(preprocess_date)
    
                for j in range(len(df)):
                    row = df.iloc[j]
                    data = tuple([uuid4(), *row])
                    session.execute(insert_query[i], data)
            except Exception as e:
                print(str(e))