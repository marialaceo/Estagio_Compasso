import sys
import re
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, explode, array
from pyspark.sql.types import DoubleType, StringType, DateType, ArrayType

# Obtendo os parâmetros do job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'JSON_INPUT_PATH', 'TRUSTED_OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Função para processar dados JSON e convertê-los para Parquet
def process_json_to_parquet(input_path, output_path):
    print(f"Processing file: {input_path}")
    try:
        # Leitura dos arquivos JSON usando Spark
        dataframe = spark.read.json(input_path)
        print(f"Initial DataFrame count: {dataframe.count()}")

        # Adicionar lógica para conversão de tipos
        dataframe = dataframe.withColumn(
            "data_lancamento", to_date(col("data_lancamento"), "yyyy-MM-dd")  # Converter a coluna 'data_lancamento' para tipo date
        ).withColumn(
            "nota_media", col("nota_media").cast(DoubleType())  # Converter a coluna 'nota_media' para tipo double
        ).withColumn(
            "popularidade", col("popularidade").cast(DoubleType())  # Converter a coluna 'popularidade' para tipo double
        ).withColumn(
            "orçamento", col("orçamento").cast(DoubleType())  # Converter a coluna 'orçamento' para tipo double
        ).withColumn(
            "receita", col("receita").cast(DoubleType())  # Converter a coluna 'receita' para tipo double
        ).withColumn(
            "bilheteira", col("bilheteira").cast(DoubleType())  # Converter a coluna 'bilheteira' para tipo double
        ).withColumn(
            "pais_producao", col("pais_producao").cast(StringType())  # Converter a coluna 'pais_producao' para tipo array de string
        ).withColumn(
            "generos", col("generos").cast(StringType())  # Converter a coluna 'generos' para tipo string
        )

        print(f"DataFrame schema after type conversions: {dataframe.schema}")
        print(f"DataFrame sample data after type conversions: {dataframe.show(5)}")

        # Escrever dados no formato Parquet sem particionamento
        print(f"Writing to: {output_path}")
        dataframe.write.mode("overwrite").parquet(output_path)
        print(f"Data written successfully to: {output_path}")

    except Exception as e:
        print(f"Error processing file {input_path}: {e}")

# Extraindo a data do caminho de entrada
input_path = args['JSON_INPUT_PATH']
date_match = re.search(r'(\d{4}/\d{2}/\d{2})', input_path)
if date_match:
    date_str = date_match.group(1)
    # Transformando a data no formato yyyy-mm-dd
    date_formatted = date_str.replace('/', '-')
else:
    raise ValueError("Data não encontrada no caminho de entrada")

# Caminho de saída com a data extraída
trusted_output_path = f"{args['TRUSTED_OUTPUT_PATH']}/filmes_json/dt={date_formatted}/"

# Processar arquivos JSON para Parquet
process_json_to_parquet(input_path, trusted_output_path)

# Finalizar job
job.commit()
