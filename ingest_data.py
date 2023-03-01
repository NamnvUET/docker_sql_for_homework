import os
from time import time
from sqlalchemy import create_engine
import argparse
import pandas as pd

#initial parser
parser = argparse.ArgumentParser(prog='Ingest Data Prog', description='Ingest CSV data from url to table_name on PostgresDB', epilog='End Program')

#Params: user, password, host, port, database name, table-name, url
parser.add_argument('--user', help='username for postgresDB')
parser.add_argument('--password', help='password for postgresDB')
parser.add_argument('--host', help='host of postgresDB')
parser.add_argument('--port', help='port of postgresDB')
parser.add_argument('--db', help='database name for postgresDB')
parser.add_argument('--yellow_taxi_table', help='name of the table where we will write the result to')
parser.add_argument('--yellow_taxi_data_url', help='url of the csv file will downloaded')
parser.add_argument('--green_taxi_table', help='name of the table where we will write the result to')
parser.add_argument('--green_taxi_data_url', help='url of the csv file will downloaded')
parser.add_argument('--zones_data_table', help='name of the table where we will write the result to')
parser.add_argument('--zones_data_url', help='url of the csv file will downloaded')

args = parser.parse_args()

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    yellow_taxi_table = params.yellow_taxi_table
    yellow_taxi_data_url = params.yellow_taxi_data_url
    green_taxi_table = params.green_taxi_table
    green_taxi_data_url = params.green_taxi_data_url
    zones_data_table = params.zones_data_table
    zones_data_url = params.zones_data_url

    csv_name_for_yellow_taxi_data = 'yellow_data.csv.gz'
    csv_name_for_green_taxi_data = 'green_data.csv.gz'
    csv_name_for_zones_data = 'zones_data.csv'

    #For test params
    #print(user,password, host, port, db, table_name, url)

    os.system(f"wget {yellow_taxi_data_url} -O {csv_name_for_yellow_taxi_data}")
    os.system(f"wget {green_taxi_data_url} -O {csv_name_for_green_taxi_data}")
    os.system(f"wget {zones_data_url} -O {csv_name_for_zones_data}")

    os.system(f"gzip -d {csv_name_for_yellow_taxi_data}")
    os.system(f"gzip -d {csv_name_for_green_taxi_data}")

    # create engine to work with sql
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')

    # For test connect
    # engine.connect()

    # Ingest Yellow Taxi Data
    df = pd.read_csv("yellow_data.csv").head(n=0)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.to_sql(name=yellow_taxi_table, con=engine, if_exists='replace')
    
    del df

    with pd.read_csv("yellow_data.csv", iterator=True, chunksize=100000) as reader:
        for chunk in reader:
            t_start = time()
            chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
            chunk.tpep_dropoff_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)
            chunk.to_sql(name=yellow_taxi_table, con=engine, if_exists='append')
            t_end = time()
            print(f'inserted chunk for {yellow_taxi_table} ..., took %.3f second' % (t_end - t_start) )
    
    # Ingest Green Taxi Data
    df = pd.read_csv("green_data.csv").head(n=0)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.to_sql(name=green_taxi_table, con=engine, if_exists='replace')
    
    del df

    with pd.read_csv("green_data.csv", iterator=True, chunksize=100000) as reader:
        for chunk in reader:
            t_start = time()
            chunk.lpep_pickup_datetime = pd.to_datetime(chunk.lpep_pickup_datetime)
            chunk.lpep_dropoff_datetime = pd.to_datetime(chunk.lpep_dropoff_datetime)
            chunk.to_sql(name=green_taxi_table, con=engine, if_exists='append')
            t_end = time()
            print(f'inserted chunk {green_taxi_table} ..., took %.3f second' % (t_end - t_start) )

    # Ingest Zones Data
    df = pd.read_csv("zones_data.csv")
    df.head(n=0).to_sql(name=zones_data_table, con=engine, if_exists='replace')
    t_start = time()
    df.to_sql(name=zones_data_table, con=engine, if_exists='append')
    t_end = time()
    print(f'inserted chunk {zones_data_table} ..., took %.3f second' % (t_end - t_start) )

if __name__ == "__main__":
    main(args)