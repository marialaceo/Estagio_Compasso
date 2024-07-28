import json
import boto3
import os
import logging
from datetime import datetime
from requests import get
from tmdbv3api import TMDb, Genre

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurar cliente S3
s3_client = boto3.client('s3',
                         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                         aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
                         region_name=os.getenv('AWS_REGION'))

bucket_name = 'data-lake-final-programa-de-bolsas'
last_page_key = 'Raw/TMDB/JSON/last_page.txt'
batch_size = 50 # Número de páginas a processar por execução

def get_last_page():
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=last_page_key)
        last_page = int(response['Body'].read().decode('utf-8'))
        logger.info(f"Última página processada lida: {last_page}")
        return last_page
    except s3_client.exceptions.NoSuchKey:
        # Se o arquivo não existir, começar do zero
        logger.info("Nenhum registro de página encontrado. Iniciando do início.")
        return 0

def save_last_page(page):
    s3_client.put_object(
        Bucket=bucket_name,
        Key=last_page_key,
        Body=str(page),
        ContentType='text/plain'
    )
    logger.info(f"Última página processada salva como {page}.")

def save_to_s3(movies, part_num):
    if not movies:
        logger.info("Nenhum filme encontrado.")
        return
    # Preparar os dados para processamento
    data = [{
        'title': movie.get('title'),
        'release_date': movie.get('release_date'),
        'popularity': movie.get('popularity'),
        'actors': [actor.get('name') for actor in movie.get('credits', {}).get('cast', [])]
    } for movie in movies]

    # Definir constantes
    max_items_per_file = 100

    # Dividir os dados em blocos de 100 itens cada
    for i in range(0, len(data), max_items_per_file):
        part_data = data[i:i + max_items_per_file]
        part_json = json.dumps(part_data, ensure_ascii=False)

        
        # Definir o caminho do arquivo para S3
        date_path = datetime.now().strftime('%Y/%m/%d')
        file_path = f'Raw/TMDB/JSON/{date_path}/movies_part_{part_num}_chunk_{i // max_items_per_file + 1}.json'

        try:

            # Salvar o arquivo no S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_path,
                Body=part_json,
                ContentType='application/json'
            )
            logger.info(f"Arquivo JSON salvo no S3: {file_path}")
        except Exception as e:
            logger.error(f"Erro ao salvar arquivo no S3: {e}")

def process_romance_movies(tmdb, s3_client, bucket_name, genre_id):
    # Obter o número total de páginas
    response = get(
        f'https://api.themoviedb.org/3/discover/movie',
        params={
            'api_key': tmdb.api_key,
            'with_genres': genre_id,
            'page': 1,
            'language': 'pt-BR',
            'sort_by': 'popularity.desc'
        }
    )
    if response.status_code == 200:
        total_pages = response.json().get('total_pages', 0)
        logger.info(f"Número total de páginas de filmes de romance: {total_pages}")
    else:
        logger.error(f"Erro ao obter o número total de páginas: {response.status_code} {response.text}")
        return

    last_page = get_last_page()
    start_page = last_page + 1
    max_pages = min(total_pages, 500) # Garantir que não exceda o número total de páginas disponível

    # Processar as páginas em lotes
    for current_page in range(start_page, max_pages + 1, batch_size):
        end_page = min(current_page + batch_size - 1, max_pages)
        logger.info(f"Iniciando o processamento de páginas {current_page} a {end_page}.")

        movies = []
        for page in range(current_page, end_page + 1):
            logger.info(f"Processando página {page} de filmes de romance...")
            response = get(
                f'https://api.themoviedb.org/3/discover/movie',
                params={
                    'api_key': tmdb.api_key,
                    'with_genres': genre_id,
                    'page': page,
                    'language': 'pt-BR',
                    'sort_by': 'popularity.desc'
                }
            )
            if response.status_code == 200:
                page_movies = response.json().get('results', [])
                logger.info(f"Encontrados {len(page_movies)} filmes na página {page}.")
                movies.extend(page_movies)
            else:
                logger.error(f"Erro ao obter dados para a página {page}: {response.status_code} {response.text}")
                break

        if movies:
            save_to_s3(movies, (current_page - 1) // batch_size + 1)
            save_last_page(end_page)

def lambda_handler(event, context):
    logger.info("Iniciando a função lambda...")
    
    # Inicializar a API do TMDb
    tmdb = TMDb()
    tmdb.api_key = os.environ.get('TMDB_API_KEY')

    # Buscar o ID do gênero de romance
    logger.info("Buscando o ID do gênero 'Romance'...")
    romance_genre_id = get_genre_id(tmdb, 'Romance')

    # Verificar se o ID foi encontrado
    if not romance_genre_id:
        logger.error("ID do gênero 'Romance' não encontrado.")
        return {
            'statusCode': 500,
            'body': json.dumps('Romance genre ID not found')
        }

    logger.info(f"ID do gênero 'Romance': {romance_genre_id}")

    # Buscar e processar filmes de romance
    logger.info("Iniciando o processamento de filmes de romance...")
    process_romance_movies(tmdb, s3_client, bucket_name, romance_genre_id)

    logger.info("Ingestão de dados concluída com sucesso.")
    return {
        'statusCode': 200,
        'body': json.dumps('Data ingestion complete')
    }

def get_genre_id(tmdb, genre_name):
    genre = Genre()

    # Obter lista de gêneros para filmes
    logger.info("Obtendo lista de gêneros para filmes...")
    movie_genres_response = genre.movie_list()
    
    # Extrair listas de gêneros
    movie_genres = [g for g in movie_genres_response.get('genres', [])]

    for g in movie_genres:
        if g['name'].lower() == genre_name.lower():
            return g['id']

    return None

if __name__ == "__main__":
    lambda_handler(None, None)
