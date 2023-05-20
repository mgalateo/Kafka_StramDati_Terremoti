# Utilizza un'immagine di base con Python preinstallato
FROM python:3.9-slim

# Imposta la directory di lavoro nel container
WORKDIR /app

# Copia il file requirements.txt nel container
COPY requirements.txt .

# Installa le dipendenze del produttore Kafka
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice del produttore Kafka nel container
COPY kafka_producer.py .

# Comando di avvio per il produttore Kafka
CMD ["python", "kafka_producer.py"]
