import pyodbc
import pandas as pd 
import os
import tweepy as tw

#Criação de variáveis e conexão com o banco

server = "."
database = "Boticario"
#username = "aula_mongodb"
#password = "abc123"
#string_conexao = 'Driver={SQL Server Native Client 11.0};Server='+server+';Database='+database+';UID='+username+';PWD='+ password
string_conexao = 'Driver={SQL Server Native Client 11.0};Server='+server+';Database='+database+';Trusted_Connection=yes;'
conexao = pyodbc.connect(string_conexao)
cursor = conexao.cursor()

#Criação tabela tweet

cursor.execute("""
IF OBJECT_ID('dbo.Tweets', 'U') IS NOT NULL 
  BEGIN
      DROP TABLE dbo.Tweets; 
      CREATE TABLE Tweets (
            ID_TWEET int IDENTITY(1,1) PRIMARY KEY,
            USUARIO varchar(255),
            TEXTO varchar(280)
        );
    END
ELSE
      CREATE TABLE Tweets (
            ID_TWEET int IDENTITY(1,1) PRIMARY KEY,
            USUARIO varchar(255),
            TEXTO varchar(280)
        );
"""
)

conexao.commit()

#Busca a linha com mais vendas em 12/2019
cursor.execute("""
SELECT TOP 1 linha from Vendas_LinhaData
where ANO = 2019 
    AND MES = 12
    AND QTD_VENDA = (SELECT MAX(QTD_VENDA) 
                        from Vendas_LinhaData 
                        where ANO = 2019 
                            AND MES = 12)
"""
)

maiorLinha = ""

while True:
        row = cursor.fetchone()
        if row == None:
            break
        maiorLinha = row.linha
        
maiorLinha

#Conexão Tweet

consumer_key= ''
consumer_secret= ''
access_token= ''
access_token_secret= ''

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

# Busca Tweets

buscaChave = "Boticário " + maiorLinha.lower()
dataDesde = "2020-01-01"

tweets = tw.Cursor(api.search,
              q=buscaChave,
              lang="pt",
              since= dataDesde).items(50)

              #Populando Base Tweets

dfTweet = pd.DataFrame(data=[[tweet.user.screen_name, tweet.text] for tweet in tweets], 
                    columns=['USUARIO', "TEXTO"])

for index, row in dfTweet.iterrows():
    cursor.execute("INSERT INTO Tweets (USUARIO, TEXTO) values(?,?)",
                   row.USUARIO, row.TEXTO)

conexao.commit()

cursor.close()
conexao.close()