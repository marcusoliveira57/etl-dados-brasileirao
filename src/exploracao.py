import pandas as pd

caminho_partidas = 'data/raw/campeonato-brasileiro-full.csv'

df_partidas = pd.read_csv(caminho_partidas)
print(df_partidas.head())

print("\n=== RESUMO DAS COLUNAS E TIPOS DE DADOS ===")
df_partidas.info()

print("\n=== QUANTIDADE DE VALORES VAZIOS POR COLUNA ===")
print(df_partidas.isnull().sum())