

# import library's .py
import yfinance as yf
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv
import os


nome_schema_pg = 'dbfinance'
nome_table_pg = 'commodities'


# Import variables de ambientes
user = os.environ.get('POSTGRES_USER')
password = os.environ.get('POSTGRES_PASSWORD')
banco = os.environ.get('POSTGRES_DB')

# Criar string de conexão
string_credentials = f"postgresql+psycopg2://{user}:{password}@localhost:5433/{banco}"

# Criar engine
engine = sa.create_engine(string_credentials)





# ---------------------------- CÓDIGO - Obter cotação dos ativos --------------------------- 
commodities = ['CL=F', 'GC=F', 'SI=F']

def buscar_dados_commodities(simbolo, periodo='5d', intervalo='1d'):
    """Função que obtém os dados da library yfinance e retorna um df de cada 'simbolo' """
    ticker = yf.Ticker(simbolo)
    dados = ticker.history(period=periodo, interval=intervalo)
    dados['simbolo'] = simbolo
    dados = dados.reset_index()
    dados['Date'] = dados['Date'].dt.date
    dados['Date'] = pd.to_datetime(dados['Date'])
    dados = dados.loc[:, ['Date', 'Open', 'High', 'Low', 'Close', 'simbolo']]

    return dados



def buscar_todos_commodites(commodities):
    """Função que é chamda pelo 'main' e itera na lista de commodities; 
       ao final retorna df c/ todos os 'simbolos' """
    
    todos_dados = []

    for simbolo in commodities:
        dados = buscar_dados_commodities(simbolo)
        todos_dados.append(dados)
        
    return pd.concat(todos_dados)



# ---------------- CONEXÃO | CHECKING REFLECTING | CRETE TABLE | INSERT -------------

def inserir_dados_banco(conexao, df):
    """Função para inserir dados no Postgres tabela"""

    conexao.execute(
        sa.text(f"INSERT INTO {nome_schema_pg}.{nome_table_pg} (data, abertura, alta, baixa, fechado, simbolo) VALUES (:data, :abertura, :alta, :baixa, :fechado, :simbolo)"),
        [{'data': reg['Date'],'abertura':reg['Open'] ,'alta':reg['High'], 'baixa':reg['Low'],'fechado': reg['Close'], 'simbolo': reg['simbolo']} for i, reg in df.iterrows()])
    conexao.commit()



def interacao_com_banco(df):
    """Função p/ interagir com o banco de dados"""

    with engine.connect() as conn:
        
        # Reflecting para checar existência da tb - Mais perfomático
        inspect = sa.inspect(engine)
        bool_table = inspect.has_table(nome_table_pg, nome_schema_pg)

        # Tabela já existe, inseri dados
        if bool_table:
            inserir_dados_banco(conn, df)

        else:
            # Obtém metadados do schema especificado
            metadata = sa.MetaData(schema=nome_schema_pg)

            # Estrutura da tb
            new_table = sa.Table(
                nome_table_pg, metadata,
                sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
                sa.Column('data', sa.Date),
                sa.Column('abertura', sa.Float),
                sa.Column('alta', sa.Float),
                sa.Column('baixa', sa.Float),
                sa.Column('fechado', sa.Float),
                sa.Column('simbolo', sa.String)
            )

            # Criar as tabelas no schema definido
            metadata.create_all(engine)

            # Inserir os dados na nova tabela
            inserir_dados_banco(conn, df)

    conn.close()  



if __name__ == "__main__":
    dados_concatenados = buscar_todos_commodites(commodities)  # concatenar meus ativos
    interacao_com_banco(dados_concatenados)