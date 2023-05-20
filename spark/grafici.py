import pandas as pd


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

data=spark.read.csv('output.csv', header=True, sep=',')
data.show(5)

from pyspark.sql.functions import col, max, coalesce
from pyspark.sql.functions import max


# MASSIMO MAGNITUDE
data = data.withColumn("Magnitude", col("Magnitude").cast("float"))

# Calcola il valore massimo nella colonna "Magnitude"
max_magnitude = data.agg({"Magnitude": "max"}).collect()[0][0]

# Filtra il dataframe per selezionare solo le righe con il valore massimo nella colonna "Magnitude"
row_with_max_magnitude = data.filter(data.Magnitude == max_magnitude)

# Visualizza la riga con il valore massimo
row_with_max_magnitude.show()

import plotly.express as px
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import to_date

data = data.withColumn("Time", to_timestamp(data.Time, "yyyy-MM-dd HH:mm:ss.SSS"))

data = data.withColumn("Latitude", col("Latitude").cast("float"))
data = data.withColumn("Longitude", col("Longitude").cast("float"))


# Seleziona le righe con il giorno "2016-11-30"
df_filtered = data.filter(to_date(data.Time) == '2016-11-30')
# Visualizza le righe filtrate
df_filtered.show()


df_filtered = df_filtered.withColumn('Time', col('Time').cast('String'))

pandasDF = df_filtered.toPandas()



fig = px.scatter_mapbox(pandasDF, lat="Latitude", lon="Longitude", size="Magnitude",
                  color_continuous_scale=px.colors.cyclical.IceFire, size_max=15, zoom=8,
                  mapbox_style="carto-positron")
fig.show()

fig.write_image('mapData.png', format='png')

from pyspark.sql.functions import col, count, to_date
# Converti la colonna 'Time' in formato data
df = data.withColumn('Time', to_date(col('Time')))

# Calcola il numero di terremoti per ogni giorno
daily_count = df.groupBy('Time').agg(count('*').alias('Count'))

# Visualizza i risultati
daily_count.show(100)

import plotly.express as px

pandasDF2 = daily_count.toPandas()

fig2 = px.histogram(pandasDF2, x="Time",y="Count",nbins=15)
fig2.show()
fig2.write_image('hysto.png', format='png')