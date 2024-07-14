# Use a imagem base do Python
FROM python:3.8-slim

# Defina o diretório de trabalho no contêiner
WORKDIR /app

RUN pip install boto3 pandas python-dotenv 

# Copie o arquivo .env para o contêiner
ENV ENV_FILE=.env

# Copie o restante do código da aplicação para o contêiner
COPY . /app

# Comando padrão para iniciar sua aplicação
CMD ["python", "ingestao.py"]


