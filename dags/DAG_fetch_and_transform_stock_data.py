from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import requests
import json
import os
import pandas as pd
import psycopg2

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'fetch_and_transform_stock_data',
    default_args=default_args,
    description='Fetches, transforms and loads sotck data into postgreSQL database.',
    schedule_interval=timedelta(days=1),
    catchup=False, 
)

#This function fetches historical data for 20 different stocks and saves them as json
def fetch_stock_data():
    #List of stock symbols that are fetched from API. Maybe later do it more dynamically?
    stock_symbol_list = ["MSFT", "GOOGL", "NET", "META", "ORCL", "NFLX", "SAP", "CRM", "IBM", "ACN", "PLTR", "NOW", "ADBE", "INTU", "SHOP", "PANW", "ADP", "SPOT", "APP", "CRWD"]
    #stock_symbol_list = ["RBLX"] #FOR TESTING

    #Get secret API key from file
    API_key = ""
    with open("/run/secrets/API_key", "r") as API_file:
       API_key = (API_file.readline().strip())

    API_key_API_ninja = ""
    with open("/run/secrets/API_key_API_ninja", "r") as API_file:
       API_key_API_ninja = (API_file.readline().strip())

    #Create json from each symbol history
    STORAGE_PATH = "/opt/airflow/data"
    for symbol in stock_symbol_list:
        #Fetching price history
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={API_key}'
        r = requests.get(url)
        data = r.json()
        file_path = os.path.join(STORAGE_PATH, f"stock_data_{symbol}.json")
        with open(file_path, "w") as json_file:
            json.dump(data, json_file, indent=4)

        #Fetching market cap
        url = f"https://api.api-ninjas.com/v1/marketcap?ticker={symbol}"
        r = requests.get(url, headers={'X-Api-Key': API_key_API_ninja})
        data = r.json()
        file_path = os.path.join(STORAGE_PATH, f"stock_data_MC_{symbol}.json")
        with open(file_path, "w") as json_file:
            json.dump(data, json_file, indent=4)

def fetch_postgres_connection_details():
    postgres_hook = PostgresHook("my_postgres_conn", )
    conn_details = postgres_hook.get_conn()
    conn_params = {
        'dbname': conn_details.info.dbname,
        'user': conn_details.info.user,
        'password': conn_details.info.password,
        'host': conn_details.info.host,
        'port': conn_details.info.port
    }

    # print(conn_params)
    # print("TESTAAMISTAA")
    # logging.info("TESTAAMISTAAA")
    logging.info(conn_params)
    return conn_params

def transform_and_update_stock_price_table():
    try:
        with psycopg2.connect(**fetch_postgres_connection_details()) as conn:
            with conn.cursor() as cur:
                #Fetch all symbols, company_id and current_price_date and create symbol list
                cur.execute("SELECT symbol, company_id, current_price_date from stock_company;")
                symbol_list = cur.fetchall()

                #Helper variable to show how many new rows are added to price history
                new_rows_price_history = 0
                STORAGE_PATH = "/opt/airflow/data"
                #Iterate trough each json and save them into database
                for symbol in symbol_list:
                    logging.info(f"Starting transforming for symbol {symbol[0]}")
                    #Load stock data into data frame
                    file_path = os.path.join(STORAGE_PATH, f"stock_data_{symbol[0].strip()}.json")
                    with open(file_path, "r") as json_file:
                        data = json.load(json_file)
                    time_series = data["Time Series (Daily)"]
                    df = pd.DataFrame.from_dict(time_series, orient="index")

                    #Deleting unnecessary references
                    del time_series
                    del data

                    #Transforming Data Frame data to be ready for inserting into SQL database
                    df.columns = ["price_open", "price_high", "price_low", "price_close", "volume"]
                    df = df.astype({"price_open": float, "price_high": float, "price_low": float, "price_close": float, "volume": float})
                    df.index.name = "date"
                    df.reset_index(inplace=True)
                    df["date"] = pd.to_datetime(df["date"])
                    df.sort_values(by="date", ascending=False, inplace=True)
                    
                    #If there is already price history, this will filter only the new rows to be added
                    if symbol[2] != None:
                        df = df[df["date"] > pd.to_datetime(symbol[2])]

                    #If there is any new rows, this will insert new rows into stock_price_history table and update some values in stock_company table
                    if len(df) > 0:
                        #Inserting new data into stock_price_history
                        for _, row in df.iterrows():
                            cur.execute('''
                                        INSERT INTO stock_price_history ("company_id", "price_date", "price_open", "price_high", "price_low", "price_close", "volume")
                                        VALUES (%s, %s, %s, %s, %s, %s, %s) 
                                        ''',
                                        (symbol[1], row["date"], row["price_open"], row["price_high"], row["price_low"], row["price_close"], row["volume"])
                                        )
                            new_rows_price_history += 1
                        logging.info(f"Saved price history for symbol {symbol[0]}")
                        #Updating some values in stock_company table
                        file_path = os.path.join(STORAGE_PATH, f"stock_data_MC_{symbol[0].strip()}.json")
                        with open(file_path, "r") as json_file:
                            data = json.load(json_file)    
                        current_price_date = df.iloc[0]["date"]
                        current_price = float(df.iloc[0]["price_close"])
                        cur.execute('''
                                    UPDATE stock_company SET current_price = %s, current_price_date = %s, market_cap = %s where symbol = %s;
                                    ''',
                                    (current_price, current_price_date, data["market_cap"], symbol[0])
                                    )
                        logging.info(f"Saved market cap for symbol {symbol[0]}")

                #For debugging purposes to show how many rows have been updated or created        
                if new_rows_price_history != 0:
                    print(f"New rows created in stock_price_history: {new_rows_price_history}\nRows updated in stock_company: {len(symbol_list)}")
                else:
                    print("No rows updated or created")

                #For testin the connetcion
                # cur.execute("SELECT NOW();")
                # print("Connected! Server time:", cur.fetchone()[0])


    except Exception as e:
        #Simple error message to describe the issue
        print(f"Database connection error: {e}")

# fetch_stock_data_task = PythonOperator(
#     task_id='fetch_stock_data_task',
#     python_callable=fetch_stock_data,
#     dag=dag,
# )

transform_and_update_stock_price_table_task = PythonOperator(
    task_id='transform_and_update_stock_price_table',
    python_callable=transform_and_update_stock_price_table,
    dag=dag,
)

# test_task = PythonOperator(
#     task_id='test',
#     python_callable=fetch_postgres_connection_details,
#     dag=dag,
# )

transform_and_update_stock_price_table_task