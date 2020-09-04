## From SQL to DataFrame Pandas
import pandas as pd
import pyodbc
import numpy as np
from timeit import default_timer as timer

#armazenar os dados da origem pelo pandas
sql_conn = pyodbc.connect("DRIVER={SQL Server};SERVER=( SERVIDOR ORIGEM );DATABASE=( BASE DE DADOS );UID=( USUARIO ) ;trusted_connection=true;") 

query = "SELECT [coluna1],[coluna2] ,[coluna3] ,[coluna4],[coluna5] FROM 'TABELA'"
df = pd.read_sql(query, sql_conn)
df = np.dtype('int32')
#df.head(3)

#gerar conexão para o destino DW
connStr = pyodbc.connect("DRIVER={SQL Server};SERVER=( SERVIDOR ORIGEM );DATABASE=( BASE DE DADOS );UID=( USUARIO );trusted_connection=true")
cursor = connStr.cursor()

#Criando função para consulta dos dados
def read(sql_conn):
    print("Consulta")
    cursor = sql_conn.cursor()
    cursor.execute("SELECT [coluna1],[coluna12] ,[coluna13] ,[coluna14],[coluna15] FROM 'TABELA';")
    for row in cursor:
        print(f'row = {row}')
    print()

  
#Leitura de dados para o insert no dw
def insert(connStr):
    print ("Insert")
    cursor = connStr.cursor()
for index,row in df.iterrows(): #lendo as linhas entre o df (pandas origem)x row (conexão destino)
 cursor.execute("INSERT INTO TABELA([coluna1],[coluna2],[coluna3],[coluna4],[coluna5]) values (?,?,?,?,?)", 
                  row['coluna1'], 
                  row['coluna2'], 
                  row['coluna3'],
                  row['coluna4'],
                  row['coluna5'])
 print(f'row = {row}')
 print()

 
  
#Comites 
connStr.commit()
 
 
#Leitura
read(sql_conn)

#Insert
insert(connStr) 
s = timer()
print('Tempo do insert',timer()-s)  
 

#fecha conexão 
cursor.close()
connStr.close()

