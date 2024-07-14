# Desafio Sprint 06

## O que o desafio pediu?

O desafio pedia que fosse incerido em um bucket aws dois arquivos csv con dados de filmes e séries, através de um código python, para isso era necessário criar um caminho para esses dois arquivos. depois deviamos criar um Dokerfile para fazer a ingestão desses arquivos atravéz de uma imagem do arquivo python. 
Minha maior dificuldade foi fazer a leitura correta das credenciais, para que não houvesse problemas com o bucket, para isso eu criei uma politica que permitia a ingestão de dados no bucket, e criei um arquivo de variáveis de ambiente para colocar minhas credenciais de forma segura, chamei eles no arquivo python e ainda chamei ele no Dokerfile, dessa forma foi possível concluir o desafio.

## Documentação

```python

import boto3
from dotenv import load_dotenv
import os
from datetime import datetime

```

Nessa primeira parte eu chamei o boto3, load_dotenv, os, datetime, que eram os módulos e bibliotecas que eu iria precisar para realizar o desafio.

```python

load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')
region_name = os.getenv('AWS_REGION')

s3_client = boto3.client('s3', 
                      aws_access_key_id=aws_access_key_id, 
                      aws_secret_access_key=aws_secret_access_key,
                      aws_session_token=aws_session_token,
                      region_name=region_name
                      )


```

Nessa parte eu chamei o arquivo .env que eu criei usando o ```load_dotenv()```, depois coloquei cada variável dentro de uma nova variável com o ```os.getenv()```, depois instanciei um cliente s3 com o ```boto3.client('s3', ...)``` e coloquei as variaveis dentro dos parâmetros.

```python

def upload_arquivo_s3(file_path, bucket, key):
    try:
        s3_client.upload_file(file_path, bucket, key)
        print(f'Sucesso no upload de {file_path} para {bucket}/{key}')
    except Exception as e:
        print(f'Erro no upload {file_path}: {e}')

```
Nessa parte eu fiz uma função para testar o upload e me devolver uma mensagem de sucesso ou falha.

```python

def main():
    bucket_name = 'data-lake-final-programa-de-bolsas'
    raw_zone = 'Raw/Files/CSV'

    now = datetime.now()
    date_path = now.strftime('%Y/%m/%d')

    files = {
        'movies.csv': 'Movies',
        'series.csv': 'Series'
    }

    for file_name, category in files.items():
        file_path = os.path.join(os.getcwd(), file_name)
        s3_key = f'{raw_zone}/{category}/{date_path}/{file_name}'
        upload_arquivo_s3(file_path, bucket_name, s3_key)

```

Depois implementei a função main, onde eu defini o nome do bucket e a primeira parte do caminho que seria criado dentro do bucket, depois eu defini a data usando o ```datetime.now()``` e estabeleci que ele deveria ter esse formato YYYY/mm/dd usando o ```now.strtime()```, logo após criei uma chave valor em que os nomes dos arquivos de upload eram a chave e suas categorias eram os valores e armazenei no dicionário ```files```.

E por último usei um ```for``` para iterar sobre o dicionário files, criei o caminho completo para o arquivo local, combinando o diretório atual ```os.getcwd()``` com o ```file_name```, criei a chave do S3 onde o arquivo será armazenado. A chave é composta por ```raw_zone```, ```category```, ```date_path``` e ```file_name```. E chamei a função ``upload_arquivo_s3()`` para fazer o upload dos arquivos junto com as pastas do caminho desejado ```upload_arquivo_s3(file_path, bucket_name, s3_key)```, e coloquei o caminho, o nome do bucket e a  chave como parâmetros.

## Perguntas que os dados irão responder 

* Top 10 filmes de romance mais assistidos entre 2000 e 2024
* Atores que mais estiveram presentes em séries de romance em cada ano de 2000 até 2024

