import sys
import re
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# Obtendo os parâmetros do job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CSV_MOVIES_PATH', 'TRUSTED_OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def process_csv_to_parquet(input_path, output_path):
    print(f"Processando arquivo: {input_path}")
    try:
        # Carregar dados do CSV com delimitador "|"
        dataframe = spark.read.format("csv") \
            .option("header", "true") \
            .option("sep", "|") \
            .load(input_path)
        print(f"Contagem inicial do DataFrame: {dataframe.count()}")

        # Filtrar por gênero "Romance"
        dataframe = dataframe.filter(dataframe["genero"].contains("Romance"))
        print(f"Contagem do DataFrame filtrado para o gênero 'Romance': {dataframe.count()}")

        # Selecionar colunas específicas e converter tipos
        dataframe = dataframe.select(
            col("id").alias("id"),  # ID
            col("tituloOriginal").alias("titulo_original"),  # Título Original
            col("genero").alias("genero"),  # Gênero
            col("notaMedia").cast("double").alias("nota_media"),  # Nota Média
            col("numeroVotos").alias("numero_votos"),  # Número de Votos
            col("titulosMaisConhecidos").alias("titulos_mais_conhecidos"),  # Títulos Mais Conhecidos
            col("anoLancamento").alias("ano_lancamento").cast("date")  # Ano de Lançamento
        )

        print(f"Esquema do DataFrame após selecionar colunas e conversões de tipo: {dataframe.schema}")
        print(f"Dados de exemplo do DataFrame após selecionar colunas e conversões de tipo: {dataframe.show(5)}")

        # Escrever dados no formato Parquet sem particionamento
        print(f"Gravando em: {output_path}")
        dataframe.write.mode("overwrite").parquet(output_path)
        print(f"Dados gravados com sucesso em: {output_path}")

    except Exception as e:
        print(f"Erro ao processar o arquivo {input_path}: {e}")

# Extraindo a data do caminho de entrada
csv_movies_path = args['CSV_MOVIES_PATH']
date_match = re.search(r'(\d{4}/\d{2}/\d{2})', csv_movies_path)
if date_match:
    date_str = date_match.group(1)
    # Transformando a data no formato yyyy-mm-dd
    date_formatted = date_str.replace('/', '-')
else:
    raise ValueError("Data não encontrada no caminho de entrada")

# Caminho de saída com a nova camada de pasta
trusted_output_path = f"{args['TRUSTED_OUTPUT_PATH']}/filmes_csv/dt={date_formatted}/"

# Processar arquivos CSV para Parquet com filtro para filmes de romance
process_csv_to_parquet(csv_movies_path, trusted_output_path)

# Finalizar job
job.commit()