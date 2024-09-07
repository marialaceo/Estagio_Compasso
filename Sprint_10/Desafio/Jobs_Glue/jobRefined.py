import sys
import re
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql.functions import col, regexp_replace, when
from pyspark.sql.types import DoubleType, IntegerType

# Obtendo os parâmetros do job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'REFINED_OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leitura dos arquivos Parquet
movies_df = spark.read.parquet(args['INPUT_PATH'])

# Transformando dados
# Removendo colchetes da coluna 'pais_producao'
movies_df = movies_df.withColumn(
    "pais_producao", regexp_replace(col("pais_producao"), '[\\[\\]]', '')
)

# Removendo colchetes da coluna 'generos'
movies_df = movies_df.withColumn(
    "generos", regexp_replace(col("generos"), '[\\[\\]]', '')
)

# Criação da Tabela Dimensão: Filmes com a coluna 'franquia'
dim_filmes_df = movies_df.withColumn(
    "franquia",
    when(col("titulo").rlike("Crepúsculo"), "Crepúsculo").otherwise("After")
).select(
    col("id_imdb"),
    col("titulo"),
    col("data_lancamento"),
    col("nota_media").cast(DoubleType()),
    col("popularidade").cast(DoubleType()),
    col("orçamento").cast(IntegerType()).alias("orcamento"),
    col("receita").cast(IntegerType()),
    col("bilheteira").cast(IntegerType()),
    col("pais_producao"),
    col("sinopse"),
    col("generos"),
    col("franquia")
)

# Gravando os dados refinados
dim_filmes_df.write.mode("overwrite").parquet(args['REFINED_OUTPUT_PATH'] + "/dim_filmes")

# Finalizando o job
job.commit()
