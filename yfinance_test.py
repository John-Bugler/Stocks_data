# -*- coding: utf-8 -*-
import pyodbc
import yfinance as yf
import pandas as pd
import time
from datetime import datetime

# Pripojeni k DB
server = 'localhost'
database = 'reports'
connection = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes')
cursor = connection.cursor()

# Tahani tickeru z DB
cursor.execute("""
    SELECT ticker 
    FROM [reports].[dbo].[revolut_stocks] 
    WHERE type = 'BUY - MARKET' 
    GROUP BY ticker 
    ORDER BY ticker ASC;
""")
portfolio_tickers = cursor.fetchall()
portfolio_tickers_cleaned = [item[0] for item in portfolio_tickers]
print("Tickery:", portfolio_tickers_cleaned)

# Funkce pro tahani dat pres yfinance
suffixes = ['', '.HA', '.L', '.SW', '.PA', '.DE', '.AS', '.MI', '.TO', '.SI']

def get_ticker_data(ticker):
    for suffix in suffixes:
        full_ticker = ticker + suffix
        print(f"Trying {ticker} with suffix {suffix}...")
        try:
            ticker_obj = yf.Ticker(full_ticker)
            data = ticker_obj.history(period='1d')
            if not data.empty:
                data['Ticker'] = full_ticker
                data['Date'] = data.index
                return data
        except Exception as e:
            print(f"Error fetching {full_ticker}: {e}")
    print(f"No data found for {ticker}.")
    return None

# Tahani dat
df_list = []

for ticker in portfolio_tickers_cleaned:
    if not ticker.isalpha():
        print(f"Skipping unknown ticker: {ticker}")
        continue
    data = get_ticker_data(ticker)
    if data is not None:
        df_list.append(data)
    time.sleep(1)  # pauza mezi dotazy (ochrana pred throttlingem)

# Spojeni vsech dat
if df_list:
    df = pd.concat(df_list)

    # Odstraneni timezone z datetime objektu (bez chyby)
    df['Date'] = df['Date'].apply(lambda x: x.tz_convert(None) if hasattr(x, 'tzinfo') and x.tzinfo is not None else x)
    df['FormattedDate'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    df['Date'] = df['FormattedDate']
    df = df.drop(columns=['FormattedDate'])
    df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    df = df.rename(columns={
        'Date': 'date',
        'Ticker': 'ticker',
        'Open': 'open_price',
        'Close': 'close_price',
        'High': 'high_price',
        'Low': 'low_price',
        'Volume': 'volume'
    })

    selected_columns = ['timestamp', 'date', 'ticker', 'open_price', 'close_price', 'high_price', 'low_price', 'volume']

    print(f"Fetched data for {len(df_list)} tickers out of {len(portfolio_tickers_cleaned)}.")

    for index, row in df.iterrows():
        values = ', '.join([f"'{row[col]}'" if isinstance(row[col], str) else str(row[col]) for col in selected_columns])
        query = f"INSERT INTO [reports].[dbo].[revolut_stocks_prices] ({', '.join(selected_columns)}) VALUES ({values})"
        print(query)
        cursor.execute(query)
        connection.commit()
else:
    print("No data fetched.")

connection.close()