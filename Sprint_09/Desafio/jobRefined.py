import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, avg, year, when, count
from pyspark.sql import functions as F

# Obtendo os parâmetros de entrada
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH1', 'INPUT_PATH2', 'REFINED_OUTPUT_PATH'])

# Inicializando o contexto do Spark e Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Lendo os arquivos Parquet das duas pastas de entrada
df1 = spark.read.parquet(args['INPUT_PATH1']).withColumnRenamed("id", "id_df1").withColumnRenamed("genero", "genero_df1")
df2 = spark.read.parquet(args['INPUT_PATH2']).withColumnRenamed("id", "id_df2").withColumnRenamed("genero", "genero_df2")

# Realizando o join pelos IDs e pelo gênero, mantendo o título de df1
merged_df = df1.join(df2, (df1.id_df1 == df2.id_df2), how="outer") \
               .select("id_df1", "titulo_original", "genero_df1", "nota_media", "numero_votos", "ano_lancamento")

# Filtrando apenas filmes do gênero Romance
merged_df = merged_df.filter(col("genero_df1").contains("Romance"))

# Substituindo todos os valores na coluna 'genero' por "Romance"
merged_df = merged_df.withColumn("genero_df1", F.lit("Romance"))

# Extraindo o ano da data de lançamento
merged_df = merged_df.withColumn("ano_lancamento_int", year(col("ano_lancamento")))

# Garantindo que não há valores nulos ou inválidos nas colunas principais
merged_df = merged_df.filter(col("ano_lancamento_int").isNotNull() & col("titulo_original").isNotNull())

# Preenchendo valores nulos na coluna de notas e número de votos com 0 (opcional)
merged_df = merged_df.fillna({'nota_media': 0, 'numero_votos': 0})

# Removendo duplicatas em caso de inconsistências no merge
merged_df = merged_df.dropDuplicates(['id_df1', 'ano_lancamento_int'])

# Criando a dimensão de filmes
dim_filme = merged_df.select(
    col("id_df1").alias("filme_id"),
    col("titulo_original"),
    col("genero_df1").alias("genero"),
    col("ano_lancamento_int").alias("ano_lancamento")
).dropDuplicates()

# Criando a dimensão de geração
dim_geracao = merged_df.withColumn(
    "geracao", 
    when((col("ano_lancamento_int") >= 1946) & (col("ano_lancamento_int") <= 1964), "Baby Boomers")
    .when((col("ano_lancamento_int") >= 1965) & (col("ano_lancamento_int") <= 1979), "Geração X")
    .when((col("ano_lancamento_int") >= 1980) & (col("ano_lancamento_int") <= 1994), "Millennials")
    .when((col("ano_lancamento_int") >= 1995) & (col("ano_lancamento_int") <= 2012), "Geração Z")
    .when((col("ano_lancamento_int") >= 2013), "Geração Alpha")
    .otherwise("Pré-Século XX")
).select("ano_lancamento_int", "geracao").dropDuplicates()

# Criando a tabela fato com métricas (popularidade, nota média) e chaves das dimensões
fato_popularidade = merged_df.join(dim_geracao, on="ano_lancamento_int", how="inner").groupBy(
    col("ano_lancamento_int"),
    col("geracao")
).agg(
    avg("numero_votos").alias("media_popularidade"),
    avg("nota_media").alias("media_nota")
)

# Adicionando classificação por período da pandemia
periodo_df = merged_df.withColumn(
    "periodo",
    when(col("ano_lancamento_int") < 2020, "Pré-Pandemia")
    .when(col("ano_lancamento_int").between(2020, 2022), "Durante-Pandemia")
    .otherwise("Pós-Pandemia")
)

# Garantindo que 'nota_media' e 'numero_votos' não são nulos
periodo_df = periodo_df.filter(col("nota_media").isNotNull() & col("numero_votos").isNotNull())

# Criando a tabela fato para os períodos da pandemia com tratamento de nulos
fato_periodo_popularidade = periodo_df.groupBy(
    col("periodo"),
    col("ano_lancamento_int")
).agg(
    avg("numero_votos").alias("media_popularidade_periodo"),
    avg("nota_media").alias("media_nota_periodo"),
    count("*").alias("total_filmes")  # Contagem de filmes por período
)

# Salvando as dimensões e tabelas fato em Parquet
dim_filme_output_path = f"{args['REFINED_OUTPUT_PATH']}/dim_filme"
dim_geracao_output_path = f"{args['REFINED_OUTPUT_PATH']}/dim_geracao"
fato_popularidade_output_path = f"{args['REFINED_OUTPUT_PATH']}/fato_popularidade"
fato_periodo_popularidade_output_path = f"{args['REFINED_OUTPUT_PATH']}/fato_periodo_popularidade"

dim_filme.write.mode("overwrite").parquet(dim_filme_output_path)
dim_geracao.write.mode("overwrite").parquet(dim_geracao_output_path)
fato_popularidade.write.mode("overwrite").parquet(fato_popularidade_output_path)
fato_periodo_popularidade.write.mode("overwrite").parquet(fato_periodo_popularidade_output_path)

# Finalizando o job
job.commit()