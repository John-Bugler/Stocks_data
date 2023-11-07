import pyodbc

server = 'localhost'
database = 'reports'

connection = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes')


cursor = connection.cursor()
cursor.execute('select * from revolut_stocks order by date asc')
for row in cursor:
    print(row)

connection.close()