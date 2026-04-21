import pandas as pd
from sqlalchemy import create_engine

string_conexao = 'postgresql+psycopg2://postgres:1234@localhost:5432/brasileirao_db'

try:
    engine = create_engine(string_conexao)

    #Lendo dados processed
    print("Lendo arquicos processados")

    #Lendo dimensões
    df_clubes = pd.read_csv('data/processed/dim_clubes.csv')
    df_atletas = pd.read_csv('data/processed/dim_atletas.csv')

    #Lendo fatos
    df_partidas = pd.read_csv('data/processed/fato_partidas.csv')
    df_estatistica = pd.read_csv('data/processed/fato_estatisticas.csv')
    df_cartoes = pd.read_csv('data/processed/fato_cartoes.csv')
    df_gols = pd.read_csv('data/processed/fato_gols.csv')

    #Enviando tabelas dimensão
    print("Enviando tabelas dimensoes")
    df_clubes.to_sql('dim_clubes', con=engine, if_exists='replace', index=False)
    df_atletas.to_sql('dim_atletas', con=engine, if_exists='replace', index=False)

    #Enviando tabelas fato
    print("Enviando tabela fato_partidas")
    df_partidas.to_sql('fato_partidas', con=engine, if_exists='replace', index=False)
    df_estatistica.to_sql('fato_estatisticas', con=engine, if_exists='replace', index=False)
    df_cartoes.to_sql('fato_cartoes', con=engine, if_exists='replace', index=False)
    df_gols.to_sql('fato_gols', con=engine, if_exists='replace', index=False)

    print("Dados carregados")

except Exception as e:
    print(f"\n Erro: {e}")