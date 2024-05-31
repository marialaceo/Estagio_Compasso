import pandas as pd
import matplotlib.pyplot as plt

# Carregar o arquivo CSV em um DataFrame
df = pd.read_csv('caminho_para_o_arquivo/googleplaystore.csv')

# Converter a coluna "Installs" para numérica removendo caracteres não numéricos
df['Installs'] = df['Installs'].str.replace('[^\d]', '', regex=True).astype(int)

# Classificar o DataFrame pelos números de instalação em ordem decrescente
df_sorted = df.sort_values(by='Installs', ascending=False)

# Selecionar os top 5 aplicativos após a classificação
top_5_apps = df_sorted.head(5)

# Criar um gráfico de barras
plt.figure(figsize=(10, 6))
plt.bar(top_5_apps['App'], top_5_apps['Installs'], color='skyblue')
plt.title('Top 5 Apps por Número de Instalação')
plt.xlabel('Aplicativo')
plt.ylabel('Número de Instalações')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()