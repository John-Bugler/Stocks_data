import pyodbc

server = 'localhost'
database = 'reports'

connection = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes')


cursor = connection.cursor()
cursor.execute("select ticker from [reports].[dbo].[revolut_stocks] where type = 'BUY - MARKET' group by ticker order by ticker asc;")

portfolio_tickers = cursor.fetchall()
print(portfolio_tickers)

portfolio_tickers_cleaned = [item[0] for item in portfolio_tickers]
print(portfolio_tickers_cleaned)

#for row in portfolio_tickers:
#    print(row)


import yfinance as yf
import pandas as pd

    
df_list = list()
for ticker in portfolio_tickers_cleaned:
    data = yf.download(ticker, group_by="Ticker", period='1d')
    data['ticker'] = ticker           # add this column because the dataframe doesn't contain a column with the ticker
    df_list.append(data)

# combine all dataframes into a single dataframe
df = pd.concat(df_list)
print(df)






connection.close()