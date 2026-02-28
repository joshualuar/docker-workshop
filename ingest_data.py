
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

def run():
    # Configuración
    pg_user, pg_pass, pg_host, pg_port, pg_db = 'root', 'root', 'localhost', 5432, 'ny_taxi'
    target_name = 'yellow_taxi_data'
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Definir tipos para evitar errores de memoria
    dtype = {"VendorID": "Int64", "passenger_count": "Int64", "PULocationID": "Int64", "DOLocationID": "Int64"}
    parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    # Leer el primer chunk para crear la tabla
    df_iter = pd.read_csv(url, dtype=dtype, parse_dates=parse_dates, iterator=True, chunksize=100000)
    
    print("Creando tabla e insertando primer bloque...")
    df = next(df_iter)
    df.to_sql(name=target_name, con=engine, if_exists='replace', index=False)

    # El resto de los bloques
    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(name=target_name, con=engine, if_exists='append', index=False)

    print("¡Ingesta terminada!")

if __name__ == '__main__':
    run()