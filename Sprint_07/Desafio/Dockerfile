# Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

ENV ENV_FILE=.env

COPY ingestaoTMDB.py /app/ingestaoTMDB.py

CMD ["python3", "ingestaoTMDB.py"]
