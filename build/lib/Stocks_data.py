# -*- coding: utf-8 -*-

# vytvoreni exace = cxfreeze stocks_data.py --target-dir dist

# knihovna pro konekteni a praci s DB
import pyodbc

server = 'localhost'
database = 'reports'

connection = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes')
cursor = connection.cursor()
# tahani z DB tickery portfolia 
cursor.execute("select ticker from [reports].[dbo].[revolut_stocks] where type = 'BUY - MARKET' group by ticker order by ticker asc;")
portfolio_tickers = cursor.fetchall()
#print(portfolio_tickers) # check formatu ulozeni seznamu tickeru



# uprava seznamu tickeru = zpusob ulozeni polozek [('AAPL',), ('ALB',),  -> odstraneni zavorek -> ['AAPL', 'ALB',
portfolio_tickers_cleaned = [item[0] for item in portfolio_tickers]
#print(portfolio_tickers_cleaned) # check formatu ulozeni seznamu tickeru po ocisteni od zavorek

# knihovna na tahani dat z yahoo finance
import yfinance as yf
# knihovna pro manipulaci s daty
import pandas as pd

    
df_list = list()  # vytvoreni prazdneho seznamu
# tahani informaci k jednotlivym stockum podle tickeru a ukladani do listu
for ticker in portfolio_tickers_cleaned:
    data = yf.download(ticker, group_by="Ticker", period='1d') # perioda = 1d -> taham denni data
    data['Ticker'] = ticker  # pridani sloupce Ticker, ten to nestahuje
    data['Date'] = data.index # pridani sloupce Date, u 1d periody ne,aji sloupce datum, to je ulozeno jako index dataframu
    df_list.append(data)   # metoda .append = pridani nove polozky do seznamu na posledni misto


# spojeni "concat" do jednoho data framu 
df = pd.concat(df_list)
print(df)

# vytiskne nazvy vsech sloupcu v dataframu
column_names = df.columns
print(column_names)


# uprava formatu datumu ve sloupci Date
# Pøevedení sloupce 'Date' na formát 'YYYY-MM-DD'
df['FormattedDate'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
# Nahrazení pùvodních hodnot novým sloupcem
df['Date'] = df['FormattedDate']
# Odstranìní pomocného sloupce 'FormattedDate', pokud není potøeba
df = df.drop(columns=['FormattedDate'])



#print(df.Ticker[0], df.Date[0], df.Open[0], df.Close[0], df.High[0], df.Low[0], df.Volume[0]) 

from datetime import datetime
df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

# prejmenovani sloupcu aby odpovidaly sloupcum v DB
df = df.rename(columns={
    'Date': 'date',
    'Ticker': 'ticker',
    'Open': 'open_price',
    'Close': 'close_price',
    'High': 'high_price',
    'Low': 'low_price',
    'Volume': 'volume'
})

# vkladam jen vybrane sloupce, v dataframu jich je vicero
selected_columns = ['timestamp', 'date', 'ticker', 'open_price', 'close_price', 'high_price', 'low_price', 'volume']

#print(df['date'])
#print(df['ticker'])

# Iterace pøes øádky DataFrame a vkládání vybraných sloupcù do databáze
for index, row in df.iterrows():
    values = ', '.join([f"'{row[col]}'" if isinstance(row[col], str) else str(row[col]) for col in selected_columns])
    query = f"INSERT INTO [reports].[dbo].[revolut_stocks_prices] ({', '.join(selected_columns)}) VALUES ({values})"
    print (query)
    cursor.execute(query)
    connection.commit()

connection.close()
