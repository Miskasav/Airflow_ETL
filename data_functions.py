import requests
import json
import pandas as pd
import psycopg2
from datetime import date

#This function fetches historical data for 20 different stocks and saves them as json
def fetch_stock_data():
    #List of stock symbols that are fetched from API. Maybe later do it more dynamically?
    stock_symbol_list = ["MSFT", "GOOGL", "NET", "META", "ORCL", "NFLX", "SAP", "CRM", "IBM", "ACN", "PLTR", "NOW", "ADBE", "INTU", "SHOP", "PANW", "ADP", "SPOT", "APP", "CRWD"]
    
    #Get secret alphavintage API key from file
    API_key_alpha_vintage = ""
    with open("./secrets/API.txt", "r") as API_file:
       API_key_alpha_vintage = (API_file.readline().strip())

    API_key_API_ninja = ""
    with open("./secrets/API_ninja_API_key.txt", "r") as API_file:
       API_key_API_ninja = (API_file.readline().strip())

    #Create json from each symbol history
    for symbol in stock_symbol_list:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={API_key_alpha_vintage}'
        r = requests.get(url)
        data = r.json()
        with open(f"./stock_data/stock_data_{symbol}.json", "w") as json_file:
            json.dump(data, json_file, indent=4)

        url = f"https://api.api-ninjas.com/v1/marketcap?ticker={symbol}"
        r = requests.get(url, headers={'X-Api-Key': API_key_API_ninja})
        data = r.json()
        with open(f"./stock_data/stock_data_MC_{symbol}.json", "w") as json_file:
            json.dump(data, json_file, indent=4)


def transform_and_update_stock_price_table():
    #Get connection configuration from json
    with open("./secrets/postgres_connection.json") as json_file:
        connection_parameters = json.load(json_file)

    try:
        with psycopg2.connect(**connection_parameters) as conn:
            with conn.cursor() as cur:
                #Fetch all symbols, company_id and current_price_date and create symbol list
                cur.execute("SELECT symbol, company_id, current_price_date from stock_company;")
                symbol_list = cur.fetchall()

                #Helper variable to show how many new rows are added to price history
                new_rows_price_history = 0

                #Iterate trough each json and save them into database
                for symbol in symbol_list:
                    #Load stock data into data frame
                    with open(f"./stock_data/stock_data_{symbol[0].strip()}.json", "r") as json_file:
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

                        #Updating some values in stock_company table
                        with open(f"./stock_data/stock_data_MC_{symbol[0].strip()}.json", "r") as json_file:
                            data = json.load(json_file)    
                        current_price_date = df.iloc[0]["date"]
                        current_price = float(df.iloc[0]["price_close"])
                        cur.execute('''
                                    UPDATE stock_company SET current_price = %s, current_price_date = %s, market_cap = %s where symbol = %s;
                                    ''',
                                    (current_price, current_price_date, data["market_cap"], symbol[0])
                                    )

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

if __name__ == "__main__":
    #fetch_stock_data()
    transform_and_update_stock_price_table()