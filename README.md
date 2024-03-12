# ConsumerDiscretionary_2

## Sprint 1
Se añade la funcionalidad de descargar datos de empresas del S&P500 del sector Consumer Discretionary,
de cualquier año seleccionado. Estos datos se descargarán a archivos de formato a elegir, dando gran versatilidad
a la hora de guardar datos.

### Modo de uso:
Para descargar datos de un año específico a un formato de archivo, se usará el siguiente comando, desde el directorio
que incluya los archivos .py de esta rama:  
    - python3 main.py <formato_archivo> <año>  
Ejemplo:  
    - python3 main json 2018

Formatos soportados:  
    - avro  
    - orc  
    - parquet  
    -csv  
    -json  
    -xlsx  

El año deberá estar entre el año 2018 y el 2024

## Sprint 2
Se añade la funcionalidad de descargar en Real Time, donde los nuevos datos de cada día se descargarán, publicarán mediante Kafka, se procesarán en Apache Nifi y finalmente se indexarán en ElasticSearch.

### Modo de uso:
El archivo real_time_run_script.py deberá ejecutarse cada día a las 6:00 AM. Este archivo descargará los datos del día anterior (los últimos datos disponibles) y los publicará en el tópico de Kafka correspondiente. El procesamiento en Nifi y ElasticSearch se realizará automátiamente

