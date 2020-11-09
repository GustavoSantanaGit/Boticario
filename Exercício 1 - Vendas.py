import pyodbc
import pandas as pd 

#Criação de variáveis e conexão com o banco

server = "."
database = "Boticario"
#username = "aula_mongodb"
#password = "abc123"
#string_conexao = 'Driver={SQL Server Native Client 11.0};Server='+server+';Database='+database+';UID='+username+';PWD='+ password
string_conexao = 'Driver={SQL Server Native Client 11.0};Server='+server+';Database='+database+';Trusted_Connection=yes;'
conexao = pyodbc.connect(string_conexao)
cursor = conexao.cursor()

# Criação das Tabelas Vendas e Agregadoras

cursor.execute("""
      IF OBJECT_ID('dbo.Vendas', 'U') IS NOT NULL 
      BEGIN
         DROP TABLE dbo.Vendas; 
              CREATE TABLE Vendas (
                ID_VENDA int IDENTITY(1,1) PRIMARY KEY,
                ID_MARCA int,
                MARCA varchar(255),
                ID_LINHA int,
                LINHA varchar(255),
                DATA_VENDA date,
                QTD_VENDA int
            );
        END
    ELSE
        CREATE TABLE Vendas (
            ID_VENDA int IDENTITY(1,1) PRIMARY KEY,
            ID_MARCA int,
            MARCA varchar(255),
            ID_LINHA int,
            LINHA varchar(255),
            DATA_VENDA date,
            QTD_VENDA int
        );
    """
)

cursor.execute("""
    IF OBJECT_ID('dbo.Vendas_AnoMes', 'U') IS NOT NULL 
      BEGIN
          DROP TABLE dbo.Vendas_AnoMes; 
          CREATE TABLE Vendas_AnoMes (
                ID_VENDA_ANOMES int IDENTITY(1,1) PRIMARY KEY,
                ANO int,
                MES int,
                QTD_VENDA int
            );
        END
    ELSE
        CREATE TABLE Vendas_AnoMes (
            ID_VENDA_ANOMES int IDENTITY(1,1) PRIMARY KEY,
            ANO int,
            MES int,
            QTD_VENDA int
        );
"""
)

cursor.execute("""
IF OBJECT_ID('dbo.Vendas_MarcaLinha', 'U') IS NOT NULL 
  BEGIN
      DROP TABLE dbo.Vendas_MarcaLinha; 
      CREATE TABLE Vendas_MarcaLinha (
            ID_VENDA_MARCALINHA int IDENTITY(1,1) PRIMARY KEY,
            ID_MARCA int,
            MARCA varchar(255),
            ID_LINHA int,
            LINHA varchar(255),
            QTD_VENDA int
        );
    END
ELSE
    CREATE TABLE Vendas_MarcaLinha (
            ID_VENDA_MARCALINHA int IDENTITY(1,1) PRIMARY KEY,
            ID_MARCA int,
            MARCA varchar(255),
            ID_LINHA int,
            LINHA varchar(255),
            QTD_VENDA int
    );
"""
)

cursor.execute("""
IF OBJECT_ID('dbo.Vendas_MarcaData', 'U') IS NOT NULL 
  BEGIN
      DROP TABLE dbo.Vendas_MarcaData; 
      CREATE TABLE Vendas_MarcaData (
            ID_VENDA_MARCADATA int IDENTITY(1,1) PRIMARY KEY,
            ID_MARCA int,
            MARCA varchar(255),
            ANO int,
            MES int,
            QTD_VENDA int
        );
    END
ELSE
    CREATE TABLE Vendas_MarcaData (
            ID_VENDA_MARCADATA int IDENTITY(1,1) PRIMARY KEY,
            ID_MARCA int,
            MARCA varchar(255),
            ANO int,
            MES int,
            QTD_VENDA int
    );
"""
)

cursor.execute("""
IF OBJECT_ID('dbo.Vendas_LinhaData', 'U') IS NOT NULL 
  BEGIN
      DROP TABLE dbo.Vendas_LinhaData; 
      CREATE TABLE Vendas_LinhaData (
            ID_VENDA_LINHADATA int IDENTITY(1,1) PRIMARY KEY,
            ID_LINHA int,
            LINHA varchar(255),
            ANO int,
            MES int,
            QTD_VENDA int
        );
    END
ELSE
    CREATE TABLE Vendas_LinhaData (
            ID_VENDA_LINHADATA int IDENTITY(1,1) PRIMARY KEY,
            ID_LINHA int,
            LINHA varchar(255),
            ANO int,
            MES int,
            QTD_VENDA int
    );
"""
)

conexao.commit()

#Carregamento das bases .csv

dfVendas2017 = pd.read_csv("Base_2017_1.csv", sep =";") 
dfVendas2018 = pd.read_csv("Base_2018_2.csv", sep =";") 
dfVendas2019 = pd.read_csv("Base_2019_3.csv", sep =";") 

dfVendasCSV = dfVendas2017.append(dfVendas2018).append(dfVendas2019)

#Populando CSV no banco de dados

for index, row in dfVendasCSV.iterrows():
    cursor.execute("INSERT INTO Vendas (ID_MARCA, MARCA, ID_LINHA, LINHA, DATA_VENDA, QTD_VENDA) values(?,?,?,?,?,?)",
                   row.ID_MARCA, row.MARCA, row.ID_LINHA,row.LINHA,row.DATA_VENDA, row.QTD_VENDA)

conexao.commit()

#Populando tabelas agregadas
cursor.execute("""
    INSERT INTO Vendas_AnoMes
    SELECT 
        YEAR(DATA_VENDA) ANO,
        MONTH(DATA_VENDA) MES,
        SUM(QTD_VENDA) QTD_VENDA
    FROM Vendas
    GROUP BY YEAR(DATA_VENDA),MONTH(DATA_VENDA)
""")

cursor.execute("""
    INSERT INTO Vendas_MarcaLinha
    SELECT 
        ID_MARCA,
        MARCA,
        ID_LINHA,
        LINHA,
        SUM(QTD_VENDA) QTD_VENDA
    FROM Vendas
    GROUP BY ID_MARCA,
            MARCA,
            ID_LINHA,
            LINHA
""")

cursor.execute("""
    INSERT INTO Vendas_MarcaData
    SELECT
        ID_MARCA,
        MARCA,
        YEAR(DATA_VENDA) ANO,
        MONTH(DATA_VENDA) MES,
        SUM(QTD_VENDA) QTD_VENDA
    FROM Vendas
    GROUP BY ID_MARCA,
            MARCA,
            YEAR(DATA_VENDA),
            MONTH(DATA_VENDA)
""")

cursor.execute("""
    INSERT INTO Vendas_LinhaData
    SELECT
        ID_LINHA,
        LINHA,
        YEAR(DATA_VENDA) ANO,
        MONTH(DATA_VENDA) MES,
        SUM(QTD_VENDA) QTD_VENDA
    FROM Vendas
    GROUP BY ID_LINHA,
            LINHA,
            YEAR(DATA_VENDA),
            MONTH(DATA_VENDA)
""")


conexao.commit()

cursor.close()
conexao.close()