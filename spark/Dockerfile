FROM apache/spark

# Imposta la directory di lavoro nel container
WORKDIR /app

COPY requirements2.txt .
# Installa le dipendenze Python personalizzate
RUN pip install --no-cache-dir -r requirements2.txt

# Copia il codice sorgente nel container
COPY grafici.py /app
COPY output.csv .


# Comando di avvio per l'applicazione Spark
CMD ["spark-submit", "--class", "main_class", "--master", "local[*]", "grafici.py"]
