

# import library's .py
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os



# import variables de ambientes

user = os.environ.get('POSTGRES_USER')
password = os.environ.get('POSTGRES_PASSWORD')
banco = os.environ.get('POSTGRES_DB')

# -------------------- CÓDIGO - Obter cotação dos ativos -------------------- 
commodities = ['CL=F', 'GC=F', 'SI=F']

def buscar_dados_commodities(simbolo, periodo='5d', intervalo='1d'):
    """Função que obtem os dados da library yfinance e retorna um df"""
    ticker = yf.Ticker(simbolo)
    dados = ticker.history(period=periodo, interval=intervalo)[['Close']]
    dados['simbolo'] = simbolo

    return dados



def buscar_todos_commodites(commodities):
    """Função que é chamda pelo 'main' e itera na lista de commodities; 
       no fim retorna um df para inserir no Postgres"""
    todos_dados = []

    for simbolo in commodities:
        dados = buscar_dados_commodities(simbolo)
        todos_dados.append(dados)

    return pd.concat(todos_dados)



if __name__ == "__main__":
    dados_concatenados = buscar_todos_commodites(commodities)  # concatenar meus ativos
    print(dados_concatenados)



# Inputar dados no Postgres


