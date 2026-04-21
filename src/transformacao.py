import pandas as pd

#Lendo os dados
caminho_partida = 'data/raw/campeonato-brasileiro-full.csv'
df_partidas = pd.read_csv(caminho_partida)

#Convertando coluna data para Datetime
df_partidas['data'] = pd.to_datetime(df_partidas['data'], dayfirst=True, errors='coerce')

#Tratando arrecadacao
df_partidas['arrecadacao'] = df_partidas['arrecadacao'].fillna(0)

#Verificando resultado
print("\nNovos tipos confirmados:")
print(df_partidas[['data', 'arrecadacao']].dtypes)

print("\n Amostra:")
print(df_partidas[['data', 'arrecadacao']].head())

### Transformando Clubes em ID ###

#Capturando os valores unicos de mandantes e visitantes
clubes_mandantes = df_partidas['mandante'].dropna().unique()
clubes_visitante = df_partidas['visitante'].dropna().unique()

#Unificando e removendo duplicatas dos clubes
todos_clubes = set(clubes_mandantes).union(set(clubes_visitante))

#Criando novo DataFrame com a lista
df_clubes = pd.DataFrame({'nome_clube': list(todos_clubes)})

#Criando coluna de ID
df_clubes['id_clube'] = range(1, len(df_clubes) + 1)

#Reorganizando ordem das colunas para ID vir primeiro
df_clubes = df_clubes[['id_clube', 'nome_clube']]

print(f"\nEncontramos {len(df_clubes)} clubes")
print("\n AMOSTRA DA TABELA DIM_CLUBES")
print(df_clubes.head)

#Salvando resultado
caminho_salvamento = 'data/processed/dim_clubes.csv'
df_clubes.to_csv(caminho_salvamento, index=False)
print("Arquivo salvo")

### Substituindo nome dos clubes por ID ###

#Substituindo nome mandante
#Fazendo um JOIN da tabela de partidas com a de clubes
df_partidas = pd.merge(df_partidas, df_clubes, left_on='mandante', right_on='nome_clube', how='left')
#O merge traz o ID, mas chama a coluna de 'id_clube'
df_partidas = df_partidas.rename(columns={'id_clube': 'id_mandante'})
#Removendo a coluna 'nome_clube'
df_partidas = df_partidas.drop(columns=['nome_clube'])

#Substituindo nome visitante
df_partidas = pd.merge(df_partidas, df_clubes, left_on='visitante', right_on='nome_clube', how='left')
df_partidas = df_partidas.rename(columns={'id_clube': 'id_visitante'})
df_partidas = df_partidas.drop(columns=['nome_clube'])

#Removendo os nomes dos clubes
df_partidas = df_partidas.drop(columns=['mandante','visitante'])

print("\n MOSTRANDO OS DADOS")
print(df_partidas[['data', 'id_mandante', 'id_visitante', 'arrecadacao']].head())

#Salvando tabela partidas
caminho_fato = 'data/processed/fato_partidas.csv'
df_partidas.to_csv(caminho_fato, index=False)
print("\n Salvamento completo")

## Processando estatistica, gols e cartões ##

#Lendo os dados
caminho_estatistica = 'data/raw/campeonato-brasileiro-estatisticas-full.csv'
df_estatistica = pd.read_csv(caminho_estatistica)
caminho_gols = 'data/raw/campeonato-brasileiro-gols.csv'
df_gols = pd.read_csv(caminho_gols)
caminho_cartoes = 'data/raw/campeonato-brasileiro-cartoes.csv'
df_cartoes = pd.read_csv(caminho_cartoes)

## Criando dimensão atletas ##
#Capturando os valores unicos de atletas
atletas_gols = df_gols['atleta'].dropna().unique()
atleta_cartoes = df_cartoes['atleta'].dropna().unique()

#Unificando e removendo duplicatas dos clubes
todos_atletas = set(atletas_gols).union(set(atleta_cartoes))

#Criando novo DataFrame com a lista
df_atletas = pd.DataFrame({'nome_atleta': list(todos_atletas)})

#Criando coluna de ID
df_atletas['id_atleta'] = range(1, len(df_atletas) + 1)

#Reorganizando ordem das colunas para ID vir primeiro
df_atletas = df_atletas[['id_atleta', 'nome_atleta']]

print(f"\nEncontramos {len(df_atletas)} atletas")
print("\n AMOSTRA DA TABELA DIM_ATLETAS")
print(df_atletas.head)

#Salvando resultado
caminho_salvamento = 'data/processed/dim_atletas.csv'
df_atletas.to_csv(caminho_salvamento, index=False)
print("Arquivo salvo")

### Tratando fato estatisttica ###
### Substituindo nome dos clubes por ID ###

#Substituindo nome clube
df_estatistica = pd.merge(df_estatistica, df_clubes, left_on='clube', right_on='nome_clube', how='left')
df_estatistica = df_estatistica.rename(columns={'id_clube': 'id_clube'})
df_estatistica = df_estatistica.drop(columns=['nome_clube', 'clube'])

df_estatistica.to_csv('data/processed/fato_estatisticas.csv', index=False)
print("\n Salvamento completo estatistica")

## Tratando Gols ##
#Substituindo nome clube
df_gols = pd.merge(df_gols, df_clubes, left_on='clube', right_on='nome_clube', how='left')
df_gols = df_gols.rename(columns={'id_clube': 'id_clube'})
df_gols = df_gols.drop(columns=['nome_clube', 'clube'])

#Substituindo nome atleta
df_gols = pd.merge(df_gols, df_atletas, left_on='atleta', right_on='nome_atleta', how='left')
df_gols = df_gols.rename(columns={'id_atleta': 'id_atleta'})
df_gols = df_gols.drop(columns=['nome_atleta', 'atleta'])

df_gols.to_csv('data/processed/fato_gols.csv', index=False)
print("\n Salvamento completo gols")

## Tratando Cartões ##
df_cartoes = pd.merge(df_cartoes, df_clubes, left_on='clube', right_on='nome_clube', how='left')
df_cartoes = df_cartoes.rename(columns={'id_clube': 'id_clube'})
df_cartoes = df_cartoes.drop(columns=['nome_clube', 'clube'])

#Substituindo nome atleta
df_cartoes = pd.merge(df_cartoes, df_atletas, left_on='atleta', right_on='nome_atleta', how='left')
df_cartoes = df_cartoes.rename(columns={'id_atleta': 'id_atleta'})
df_cartoes = df_cartoes.drop(columns=['nome_atleta', 'atleta'])

df_gols.to_csv('data/processed/fato_cartoes.csv', index=False)
print("\n Salvamento completo cartoes")