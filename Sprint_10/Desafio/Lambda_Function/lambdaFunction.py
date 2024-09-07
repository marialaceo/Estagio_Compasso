import os
import json
import boto3
import requests
import logging
from datetime import datetime

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializando o cliente S3
s3_client = boto3.client('s3')

# IDs e títulos dos filmes das sagas Crepúsculo e After
twilight_after_ids_titles = {
    8966: "Crepúsculo",
    18239: "Lua Nova",
    24021: "Eclipse",
    50619: "Amanhecer - Parte 1",
    50620: "Amanhecer - Parte 2",
    537915: "After",
    613504: "After: Depois da Verdade",
    744275: "After: Depois do Desencontro",
    744276: "After: Depois da Promessa",
    820525: "After: Para Sempre (After Everything)"
}

# Função principal do Lambda
def lambda_handler(event, context):
    api_key_tmdb = os.getenv('TMDB_API_KEY')
    s3_bucket_name = os.getenv('S3_BUCKET_NAME')

    detalhes_filmes = []

    for movie_id, movie_title in twilight_after_ids_titles.items():
        detalhes_tmdb = obter_detalhes_tmdb(movie_id, api_key_tmdb)
        
        if not detalhes_tmdb:
            # Se não obtiver detalhes pelo ID, tentar buscar pelo título
            detalhes_tmdb = buscar_filme_por_titulo(movie_title, api_key_tmdb)
        
        if detalhes_tmdb and validar_detalhes(detalhes_tmdb):
            detalhes_filmes.append(detalhes_tmdb)
        else:
            logger.warning(f"Detalhes do filme não encontrados ou incompletos para ID {movie_id} e título '{movie_title}'")

    if detalhes_filmes:
        # Particionando os dados em arquivos JSON de até 100 filmes
        salvar_dados_particionados(detalhes_filmes, s3_bucket_name)

    return {
        'statusCode': 200,
        'body': json.dumps('Processamento concluído com sucesso!')
    }

# Função para obter detalhes do TMDB pelo ID
def obter_detalhes_tmdb(movie_id, api_key_tmdb):
    url_tmdb = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key_tmdb}&language=pt-BR"
    response_tmdb = requests.get(url_tmdb)

    if response_tmdb.status_code == 200:
        detalhes_tmdb = response_tmdb.json()

        # Montando o dicionário com os dados desejados em português
        return {
            'id': detalhes_tmdb['id'],
            'titulo': detalhes_tmdb['title'],
            'data_lancamento': detalhes_tmdb['release_date'],
            'nota_media': detalhes_tmdb['vote_average'],
            'popularidade': detalhes_tmdb['popularity'],
            'id_imdb': detalhes_tmdb.get('imdb_id', ''),
            'orçamento': str(detalhes_tmdb.get('budget', 'Não Disponível')),
            'receita': str(detalhes_tmdb.get('revenue', 'Não Disponível')),
            'bilheteira': str(detalhes_tmdb.get('revenue', 'Não Disponível')),
            'pais_producao': [pais['name'] for pais in detalhes_tmdb.get('production_countries', [])],
            'sinopse': detalhes_tmdb.get('overview', ''),
            'generos': [genero['name'] for genero in detalhes_tmdb.get('genres', [{'name': 'Não Disponível'}])]
        }
    else:
        logger.error(f"Erro ao obter detalhes do TMDB para o ID {movie_id}: {response_tmdb.status_code}")
        return None

# Função para buscar um filme pelo título
def buscar_filme_por_titulo(title, api_key_tmdb):
    url_tmdb = f"https://api.themoviedb.org/3/search/movie?api_key={api_key_tmdb}&query={title}&language=pt-BR"
    response_tmdb = requests.get(url_tmdb)

    if response_tmdb.status_code == 200:
        resultados = response_tmdb.json().get('results', [])
        for resultado in resultados:
            if resultado['title'] == title:
                return obter_detalhes_tmdb(resultado['id'], api_key_tmdb)
    else:
        logger.error(f"Erro ao buscar filme pelo título '{title}': {response_tmdb.status_code}")
    
    return None

# Função para validar se os detalhes do filme possuem todos os campos necessários não nulos
def validar_detalhes(detalhes):
    return (detalhes.get('titulo') and
            detalhes.get('data_lancamento') and
            detalhes.get('nota_media')  and
            detalhes.get('popularidade') and
            detalhes.get('id_imdb') and
            detalhes.get('orçamento') and
            detalhes.get('receita') and
            detalhes.get('bilheteira') and
            detalhes.get('pais_producao') and
            detalhes.get('sinopse') and
            detalhes.get('generos'))

# Função para salvar dados particionados no S3
def salvar_dados_particionados(dados, bucket_name):
    max_itens_por_arquivo = 100
    data_atual = datetime.now().strftime("%Y/%m/%d")

    # Dividindo os dados em blocos de 100 filmes
    for i in range(0, len(dados), max_itens_por_arquivo):
        parte_dados = dados[i:i + max_itens_por_arquivo]
        parte_json = json.dumps(parte_dados, ensure_ascii=False)

        caminho_s3 = f"Raw/TMDB/JSON/{data_atual}/filmes_saga_parte_{i // max_itens_por_arquivo + 1}.json"

        try:
            s3_client.put_object(Body=parte_json, Bucket=bucket_name, Key=caminho_s3)
            logger.info(f"Arquivo salvo com sucesso no S3: {caminho_s3}")
        except Exception as e:
            logger.error(f"Erro ao salvar o arquivo no S3: {e}")
