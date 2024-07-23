import os
import json
import boto3
from tmdbv3api import TMDb, Movie, TV, Genre
import datetime
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    logger.info("Iniciando a função lambda...")
    
    # Inicializar a API do TMDb
    tmdb = TMDb()
    tmdb.api_key = os.environ.get('TMDB_API_KEY')
    
    # Cliente S3
    s3_client = boto3.client('s3', 
                      aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                      aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
                      region_name=os.getenv('AWS_REGION')
                      )
    
    bucket_name = 'data-lake-final-programa-de-bolsas'
    
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
    
    # Buscar e processar séries de romance
    logger.info("Iniciando o processamento de séries de romance...")
    process_romance_series(tmdb, s3_client, bucket_name, romance_genre_id)
    
    logger.info("Ingestão de dados concluída com sucesso.")
    return {
        'statusCode': 200,
        'body': json.dumps('Data ingestion complete')
    }

def get_genre_id(tmdb, genre_name):
    genre = Genre()
    # Obter lista de gêneros para filmes e séries
    logger.info("Obtendo lista de gêneros para filmes e séries...")
    movie_genres_response = genre.movie_list()
    tv_genres_response = genre.tv_list()
    
    # Extrair listas de gêneros
    movie_genres = [g for g in movie_genres_response.get('genres', [])]
    tv_genres = [g for g in tv_genres_response.get('genres', [])]
    
    # Combinar listas de gêneros
    all_genres = movie_genres + tv_genres
    
    for g in all_genres:
        if g['name'].lower() == genre_name.lower():
            return g['id']
    
    return None

def process_romance_movies(tmdb, s3_client, bucket_name, genre_id):
    movie = Movie()
    data = []
    page = 1
    
    while page <= 500:
        logger.info(f"Processando página {page} de filmes de romance...")
        try:
            # Obter filmes populares
            response = movie.popular(page=page)
            results = response.get('results', [])
            
            if not results:
                logger.info("Nenhum resultado encontrado na página atual. Terminando a busca.")
                break
            
            for item in results:
                # Filtrar por gênero
                genre_ids = item.get('genre_ids', [])
                if genre_id in genre_ids:
                    movie_data = {
                        'id': item.get('id'),
                        'title': item.get('title'),
                        'release_date': item.get('release_date'),
                        'popularity': item.get('popularity'),
                        'genres': item.get('genre_ids')
                    }
                    data.append(movie_data)
            
            page += 1
        
        except Exception as e:
            logger.error(f"Erro ao obter filmes populares: {e}")
            break
    
    if data:
        logger.info(f"Salvando {len(data)} filmes de romance no S3.")
        save_to_s3(s3_client, bucket_name, 'movies', data)
    else:
        logger.info("Nenhum filme de romance encontrado.")

def process_romance_series(tmdb, s3_client, bucket_name, genre_id):
    tv = TV()
    data = []
    page = 1
    
    while page <= 500:
        logger.info(f"Processando página {page} de séries de romance...")
        try:
            # Obter séries populares
            response = tv.popular(page=page)
            results = response.get('results', [])
            
            if not results:
                logger.info("Nenhum resultado encontrado na página atual. Terminando a busca.")
                break
            
            for item in results:
                # Filtrar por gênero
                genre_ids = item.get('genre_ids', [])
                if genre_id in genre_ids:
                    series_data = {
                        'id': item.get('id'),
                        'title': item.get('name'),
                        'release_date': item.get('first_air_date'),
                        'popularity': item.get('popularity'),
                        'genres': item.get('genre_ids')
                    }
                    data.append(series_data)
            
            page += 1
        
        except Exception as e:
            logger.error(f"Erro ao obter séries populares: {e}")
            break
    
    if data:
        logger.info(f"Salvando {len(data)} séries de romance no S3.")
        save_to_s3(s3_client, bucket_name, 'series', data)
    else:
        logger.info("Nenhuma série de romance encontrada.")

def save_to_s3(s3_client, bucket_name, data_type, data):
    # Limitar o tamanho do arquivo a 10 MB
    MAX_SIZE_MB = 10
    data_str = json.dumps(data, default=str)
    
    if len(data_str) > MAX_SIZE_MB * 1024 * 1024:
        # Se o tamanho excede 10 MB, divida os dados em partes menores
        logger.info("Os dados excedem 10 MB, dividindo em partes menores...")
        for i in range(0, len(data_str), MAX_SIZE_MB * 1024 * 1024):
            part_data = data_str[i:i + MAX_SIZE_MB * 1024 * 1024]
            today = datetime.datetime.now()
            date_path = today.strftime("%Y/%m/%d")
            file_name = f'Raw/TMDB/JSON/{date_path}/{data_type}_part_{i//(MAX_SIZE_MB * 1024 * 1024)}.json'
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=part_data
            )
    else:
        # Se o tamanho está dentro do limite, envie o arquivo normalmente
        today = datetime.datetime.now()
        date_path = today.strftime("%Y/%m/%d")
        file_name = f'Raw/TMDB/JSON/{date_path}/{data_type}.json'
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=data_str
        )

if __name__ == "__main__":
    lambda_handler(None, None)
