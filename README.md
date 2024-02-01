# ConsumerDiscretionary_2

## Sprint 1
Se añade la funcionalidad de descargar datos de empresas del S&P500 del sector Consumer Discretionary,
de cualquier año seleccionado. Estos datos se descargarán a archivos de formato a elegir, dando gran versatilidad
a la hora de guardar datos.

### Modo de uso:
Para descargar datos de un año específico a un formato de archivo, se usará el siguiente comando, desde el directorio
que incluya los archivos .py de esta rama:  
    - python3 main.py <formato_archivo> <año>

Formatos soportados:  
    - avro  
    - orc  
    - parquet  
    -csv  
    -json  
    -xlsx  
