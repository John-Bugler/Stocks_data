# -*- coding: utf-8 -*-

# Knihovna pro konekteni a praci s DB
import pyodbc

server = 'localhost'
database = 'reports'

connection = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes')
cursor = connection.cursor()
# Tahání z DB tickery portfolia 
cursor.execute("select ticker from [reports].[dbo].[revolut_stocks] where type = 'BUY - MARKET' group by ticker order by ticker asc;")
portfolio_tickers = cursor.fetchall()

# Úprava seznamu tickerů = způsob uložení položek [('AAPL',), ('ALB',),  -> odstraneni zavorek -> ['AAPL', 'ALB',
portfolio_tickers_cleaned = [item[0] for item in portfolio_tickers]

# Knihovna na tahání dat z yahoo finance
import yfinance as yf
# Knihovna pro manipulaci s daty
import pandas as pd

# Seznam možných suffixů pro burzy
suffixes = ['', '.HA', '.L', '.SW', '.PA', '.DE', '.AS', '.MI', '.TO', '.SI']

def get_ticker_data(ticker):
    for suffix in suffixes:
        full_ticker = ticker + suffix
        data = yf.download(full_ticker, group_by="Ticker", period='1d')
        if not data.empty:
            data['Ticker'] = full_ticker  # Přidání sloupce Ticker s plným názvem
            data['Date'] = data.index      # Přidání sloupce Date
            return data
    print(f"No data found for {ticker} with any suffix.")
    return None

df_list = list()  # Vytvoření prázdného seznamu

# Tahání informací k jednotlivým stockům podle tickeru a ukládání do listu
for ticker in portfolio_tickers_cleaned:
    data = get_ticker_data(ticker)
    if data is not None:
        df_list.append(data)   # Metoda .append = přidání nové položky do seznamu na poslední místo

# Spojení "concat" do jednoho dataframe
df = pd.concat(df_list) if df_list else pd.DataFrame()  # Kontrola, jestli není seznam prázdný

if not df.empty:
    print(df)

    # Vytiskne názvy všech sloupců v dataframe
    column_names = df.columns
    print(column_names)

    # Úprava formátu datumu ve sloupci Date
    df['FormattedDate'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    df['Date'] = df['FormattedDate']
    df = df.drop(columns=['FormattedDate'])

    from datetime import datetime
    df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    # Přejmenování sloupců, aby odpovídaly sloupcům v DB
    df = df.rename(columns={
        'Date': 'date',
        'Ticker': 'ticker',
        'Open': 'open_price',
        'Close': 'close_price',
        'High': 'high_price',
        'Low': 'low_price',
        'Volume': 'volume'
    })

    # Vkládám jen vybrané sloupce, v dataframe je více sloupců
    selected_columns = ['timestamp', 'date', 'ticker', 'open_price', 'close_price', 'high_price', 'low_price', 'volume']

    # Iterace přes řádky DataFrame a vkládání vybraných sloupců do databáze
    for index, row in df.iterrows():
        values = ', '.join([f"'{row[col]}'" if isinstance(row[col], str) else str(row[col]) for col in selected_columns])
        query = f"INSERT INTO [reports].[dbo].[revolut_stocks_prices] ({', '.join(selected_columns)}) VALUES ({values})"
        print(query)
        cursor.execute(query)
        connection.commit()

connection.close()
