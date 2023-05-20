# Utilizza un'immagine di base con Python preinstallato
FROM python:3.9-slim

# Imposta la directory di lavoro nel container
WORKDIR /app

# Copia il file requirements.txt nel container
COPY requirements.txt .

# Installa le dipendenze del consumatore Kafka
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice del consumatore Kafka nel container
COPY kafka_consumer.py .

# Copia il file italy_earthquakes.csv nel container
COPY italy_earthquakes.csv .

# Comando di avvio per il consumatore Kafka
CMD ["python", "kafka_consumer.py"]
