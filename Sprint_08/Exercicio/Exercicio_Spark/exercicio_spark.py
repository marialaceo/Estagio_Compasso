from pyspark.sql import SparkSession
from pyspark import SparkContext, SQLContext

spark = SparkSession.builder.master("local[*]").appName("Exercicio Intro").getOrCreate()

df_nomes = spark.read.csv("/home/marialaceo/Documentos/desafio_remoto/nomes_aleatorios.txt", header=False)

df_nomes.show(5)
df_nomes.printSchema()

df_nomes = df_nomes.withColumnRenamed("_c0", "Nomes")

df_nomes.show(10)

from pyspark.sql.functions import lit, when, rand

df_nomes = df_nomes.withColumn("Escolaridade",when(rand() < 0.33, lit("Fundamental")).when(rand() < 0.66, lit("Médio")).otherwise(lit("Superior")))

import random

paises = ["Brasil", "Argentina", "Chile", "Uruguai", "Paraguai", "Bolívia", "Peru", "Equador", "Colômbia", "Venezuela", "Guiana", "Suriname", "Guiana Francesa"]

df_nomes = df_nomes.withColumn("Pais",lit(random.choice(paises)))

df_nomes = df_nomes.withColumn("AnoNascimento",lit(random.randint(1945, 2010)))

df_select = df_nomes.filter(df_nomes.AnoNascimento > 2000)

df_select.show(10)

df_nomes.createOrReplaceTempView("pessoas")

df_select_sql = spark.sql("SELECT * FROM pessoas WHERE AnoNascimento >= 2000")

df_select_sql.show(10)

count_millennials = df_nomes.filter((df_nomes.AnoNascimento >= 1980) & (df_nomes.AnoNascimento <= 1994)).count()

print(f"Número de Millennials: {count_millennials}")

count_millennials_sql = spark.sql("SELECT COUNT(*) as NumMillennials FROM pessoas WHERE AnoNascimento BETWEEN 1980 AND 1994")

count_millennials_sql.show()

from pyspark.sql.functions import when

df_nomes = df_nomes.withColumn("Geracao", when((df_nomes.AnoNascimento >= 1944) & (df_nomes.AnoNascimento <= 1964), "Baby Boomers").when((df_nomes.AnoNascimento >= 1965) & (df_nomes.AnoNascimento <= 1979), "Geração X").when((df_nomes.AnoNascimento >= 1980) & (df_nomes.AnoNascimento <= 1994), "Millennials").when((df_nomes.AnoNascimento >= 1995) & (df_nomes.AnoNascimento <= 2015), "Geração Z"))

df_nomes.createOrReplaceTempView("pessoas")

df_contagem_geracao = spark.sql("""
    SELECT Pais, Geracao, COUNT(*) as Quantidade
    FROM pessoas
    WHERE Geração IS NOT NULL
    GROUP BY Pais, Geração
    ORDER BY Pais ASC, Geração ASC, Quantidade ASC
""")

df_contagem_geracao.show()





