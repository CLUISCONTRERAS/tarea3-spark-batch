# Procesamiento batch con Apache Spark - Tarea 3 (Big Data, UNAD)

## Descripción del problema
Durante la pandemia de COVID-19, entender dónde y en qué grupos de población se concentraron los contagios ayuda a orientar decisiones de salud pública. Este proyecto analiza, con Apache Spark, los casos positivos registrados en Colombia para responder: ¿cómo se distribuyeron los casos por departamento, grupo de edad, sexo y tipo de contagio, y qué patrones aparecen en el tiempo?

## Conjunto de datos
- **Nombre:** Casos positivos de COVID-19 en Colombia
- **Fuente:** datos.gov.co (Instituto Nacional de Salud) - https://www.datos.gov.co/resource/gt2j-8ykr.csv
- **Tamaño:** 500.000 filas descargadas (498.397 después de la limpieza) y 23 columnas. El script descarga las primeras 500.000 filas desde la API; el dataset completo es mucho mayor
- **Columnas usadas:** `departamento_nom`, `edad`, `unidad_medida`, `sexo`, `estado`, `fuente_tipo_contagio`, `fecha_de_notificaci_n`

## Qué hace el código
`analisis_batch.py` realiza el procesamiento batch con PySpark (DataFrames):

1. **Carga:** descarga el CSV por páginas desde la API de datos.gov.co y lo lee con Spark.
2. **Limpieza y transformación:** elimina espacios y duplicados, conserva solo las edades en años (`unidad_medida = 1`), convierte tipos, genera año, mes y grupos de edad, y descarta nulos en las columnas clave.
3. **EDA:** nulos por columna, estadísticas de la edad, ranking por departamento, conteos por sexo, estado y tipo de contagio, grupos de edad y evolución mensual.
4. **Visualización:** guarda gráficas (PNG) y tablas (CSV) en la carpeta `outputs/`.

## Requisitos
- Python 3.9 o superior
- Java 8, 11 o 17 (Spark lo necesita; verifica con `java -version`)

## Instrucciones de ejecución
```bash
git clone https://github.com/CLUISCONTRERAS/tarea3-spark-batch.git
cd tarea3-spark-batch
python -m venv .venv
source .venv/bin/activate        # En Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

El script ya viene configurado para este dataset. Para usar otro, edita la sección **CONFIGURACIÓN** de `analisis_batch.py`. Luego ejecuta:

```bash
python analisis_batch.py
```

## Resultados
Las gráficas y tablas quedan en `outputs/`:
- `01_ranking_categorias.png`
- `02_distribucion_numerica.png`
- `03_serie_mensual.png`
- `04_grupos.png` (casos por grupo de edad)
- `05_sexo.png`, `06_estado.png`, `07_fuente_tipo_contagio.png`
- archivos `resultado_*.csv` con las tablas

Resultados sobre el subconjunto analizado (aprox. 498.000 registros):

- Bogotá concentra cerca del 29 % de los registros, seguida de Antioquia y Valle.
- La edad promedio es de unos 40 años, y el grupo de 30 a 44 años es el más numeroso (cerca del 32 %).
- Las mujeres son alrededor del 52 % de los casos.
- Cerca del 68 % de los contagios son de tipo comunitario.

## Limitaciones

- **Subconjunto, no muestra aleatoria.** Se analizan las primeras 500.000 filas que entrega la API en su orden interno, por lo que la evolución mensual no representa la evolución real de la pandemia en el país.
- **Edades en años.** Se filtró `unidad_medida = 1`, por lo que los menores de un año (registrados en meses o días) quedan fuera.
- **Distritos como departamentos.** Bogotá, Barranquilla y Cartagena aparecen separados de Cundinamarca, Atlántico y Bolívar, tal como vienen en la fuente.
- **Limpieza de categorías.** Se unificaron mayúsculas y minúsculas (por ejemplo `f` y `F`).

## Autor
Luis Contreras - Curso Big Data (202016911) - UNAD
