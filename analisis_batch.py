"""
Procesamiento batch con Apache Spark (PySpark)
Curso Big Data - Tarea 3
Dataset: Casos positivos de COVID-19 en Colombia (datos.gov.co, id gt2j-8ykr)

Flujo: cargar -> limpiar/transformar -> EDA -> visualizar.
Para otro dataset basta con editar la sección CONFIGURACIÓN.
"""
import csv
import io
import os
import urllib.request

import matplotlib
matplotlib.use("Agg")  # guarda las gráficas en archivos, sin abrir ventanas
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

# ============================ CONFIGURACIÓN ============================
# Fuente original (API de datos abiertos). Se descarga por páginas.
DATA_URL = "https://www.datos.gov.co/resource/gt2j-8ykr.csv"
MAX_FILAS = 500_000              # cuántas filas descargar (el dataset completo es mucho mayor)
FILAS_POR_PAGINA = 50_000        # tamaño de cada petición a la API
DATA_PATH = "data/dataset.csv"   # archivo local donde se guarda

COL_CATEGORIA = "departamento_nom"       # columna de texto para agrupar
COL_NUMERICA = "edad"                    # columna numérica a analizar
COL_FECHA = "fecha_de_notificaci_n"      # columna de fecha (o None)
FORMATO_FECHA = None                     # ej. "yyyy-MM-dd"; None = automático

# Filtro opcional: en este dataset la edad solo está en años cuando unidad_medida = 1
# (2 y 3 corresponden a meses y días). Verifícalo en el diccionario del dataset.
FILTRO = "unidad_medida = 1"             # o None para no filtrar

# Columnas adicionales para conteos (opcional)
COLS_EXTRA = ["sexo", "estado", "fuente_tipo_contagio"]

# Rangos para agrupar la columna numérica (opcional; [] para omitir)
CORTES = [0, 18, 30, 45, 60, 200]
ETIQUETAS = ["0-17", "18-29", "30-44", "45-59", "60+"]

TOP_N = 10                       # categorías a mostrar en el ranking
OUTPUT_DIR = "outputs"           # aquí se guardan tablas y gráficas
# =======================================================================


def descargar_datos():
    """Descarga los datos desde la fuente original, por páginas, si no existen."""
    if os.path.exists(DATA_PATH):
        print(f"[INFO] Usando archivo local: {DATA_PATH}")
        return
    if not DATA_URL:
        raise FileNotFoundError(
            f"No existe {DATA_PATH} y DATA_URL está vacío. "
            "Define DATA_URL o coloca el archivo en esa ruta."
        )
    os.makedirs(os.path.dirname(DATA_PATH) or ".", exist_ok=True)
    print(f"[INFO] Descargando hasta {MAX_FILAS:,} filas desde {DATA_URL} ...")

    total, offset, primera = 0, 0, True
    with open(DATA_PATH, "wb") as salida:
        while total < MAX_FILAS:
            limite = min(FILAS_POR_PAGINA, MAX_FILAS - total)
            url = f"{DATA_URL}?$limit={limite}&$offset={offset}&$order=:id"
            with urllib.request.urlopen(url, timeout=120) as resp:
                contenido = resp.read()
            # Cuenta las filas reales con el módulo csv (respeta saltos de línea entre comillas)
            n = sum(1 for _ in csv.reader(io.StringIO(contenido.decode("utf-8")))) - 1
            if n <= 0:  # ya no hay más filas
                break
            if not primera:  # quita el encabezado repetido
                contenido = contenido.split(b"\n", 1)[1]
            salida.write(contenido)
            if not contenido.endswith(b"\n"):
                salida.write(b"\n")
            primera = False
            total += n
            offset += n
            print(f"[INFO]   {total:,} filas descargadas")
            if n < limite:
                break
    print("[INFO] Descarga completa.")


def cargar(spark):
    """Carga el CSV en un DataFrame de Spark."""
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .option("multiLine", True)
          .option("escape", '"')
          .csv(DATA_PATH))
    print(f"[CARGA] Filas: {df.count():,} | Columnas: {len(df.columns)}")
    df.printSchema()
    return df


def validar_columnas(df):
    """Verifica que las columnas configuradas existan en el dataset."""
    requeridas = ([COL_CATEGORIA, COL_NUMERICA] + ([COL_FECHA] if COL_FECHA else [])
                  + COLS_EXTRA)
    faltan = [c for c in requeridas if c not in df.columns]
    if faltan:
        raise ValueError(
            f"Columnas no encontradas: {faltan}. Columnas disponibles: {df.columns}"
        )


def limpiar(df):
    """Limpieza y transformación."""
    n_inicial = df.count()

    # 1. Quitar espacios sobrantes en columnas de texto
    for nombre, tipo in df.dtypes:
        if tipo == "string":
            df = df.withColumn(nombre, F.trim(F.col(nombre)))

    # 1b. Unificar mayúsculas en las columnas categóricas (ej. 'f' y 'F', 'leve' y 'Leve')
    for c in [COL_CATEGORIA] + COLS_EXTRA:
        df = df.withColumn(c, F.upper(F.col(c)))

    # 2. Eliminar filas duplicadas
    df = df.dropDuplicates()

    # 3. Filtro de consistencia (ej. edad solo en años)
    if FILTRO:
        df = df.filter(F.expr(FILTRO))

    # 4. Convertir la columna numérica a double (valores inválidos pasan a null)
    df = df.withColumn(COL_NUMERICA, F.col(COL_NUMERICA).cast("double"))

    # 5. Convertir la fecha y crear año y mes
    if COL_FECHA:
        fecha = (F.to_date(F.col(COL_FECHA), FORMATO_FECHA) if FORMATO_FECHA
                 else F.to_date(F.col(COL_FECHA)))
        df = (df.withColumn(COL_FECHA, fecha)
                .withColumn("anio", F.year(COL_FECHA))
                .withColumn("mes", F.month(COL_FECHA)))

    # 6. Crear grupos a partir de la columna numérica (ej. grupos de edad)
    if CORTES:
        expr = None
        for i, etiqueta in enumerate(ETIQUETAS):
            cond = (F.col(COL_NUMERICA) >= CORTES[i]) & (F.col(COL_NUMERICA) < CORTES[i + 1])
            expr = F.when(cond, etiqueta) if expr is None else expr.when(cond, etiqueta)
        df = df.withColumn("grupo_numerica", expr)

    # 7. Eliminar filas con nulos en las columnas clave
    claves = [COL_CATEGORIA, COL_NUMERICA] + ([COL_FECHA] if COL_FECHA else [])
    df = df.dropna(subset=claves)

    n_final = df.count()
    print(f"[LIMPIEZA] Filas antes: {n_inicial:,} | después: {n_final:,} "
          f"| eliminadas: {n_inicial - n_final:,}")
    return df


def eda(df):
    """Análisis exploratorio. Devuelve las tablas de resultados."""
    resultados = {}

    print("\n[EDA] Nulos por columna:")
    nulos = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c)
                       for c in df.columns])
    nulos.show(truncate=False)

    print("[EDA] Estadísticas de la columna numérica:")
    df.select(COL_NUMERICA).describe().show()

    print(f"[EDA] Top {TOP_N} de {COL_CATEGORIA}:")
    ranking = (df.groupBy(COL_CATEGORIA)
                 .agg(F.count("*").alias("registros"),
                      F.round(F.avg(COL_NUMERICA), 2).alias("promedio"))
                 .orderBy(F.desc("registros"))
                 .limit(TOP_N))
    ranking.show(truncate=False)
    resultados["ranking"] = ranking.toPandas()

    resultados["extra"] = {}
    for c in COLS_EXTRA:
        print(f"[EDA] Conteo por {c}:")
        t = (df.groupBy(c).count().orderBy(F.desc("count")).limit(TOP_N))
        t.show(truncate=False)
        resultados["extra"][c] = t.toPandas()

    if CORTES:
        print("[EDA] Registros por grupo:")
        g = df.groupBy("grupo_numerica").count()
        g.show()
        gp = g.toPandas()
        gp["orden"] = gp["grupo_numerica"].map({e: i for i, e in enumerate(ETIQUETAS)})
        resultados["grupos"] = gp.sort_values("orden")

    if COL_FECHA:
        print("[EDA] Evolución mensual:")
        mensual = (df.groupBy("anio", "mes")
                     .agg(F.count("*").alias("registros"))
                     .orderBy("anio", "mes"))
        mensual.show(24, truncate=False)
        resultados["mensual"] = mensual.toPandas()

    # Muestra acotada de la columna numérica para el histograma
    resultados["muestra"] = (df.select(COL_NUMERICA)
                               .limit(100_000).toPandas()[COL_NUMERICA])
    return resultados


def _guardar(nombre):
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/{nombre}", dpi=150)
    plt.close()


def visualizar(res):
    """Genera y guarda las gráficas y las tablas en OUTPUT_DIR."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Ranking de categorías
    r = res["ranking"].sort_values("registros")
    plt.figure(figsize=(9, 5))
    plt.barh(r[COL_CATEGORIA].astype(str), r["registros"])
    plt.title(f"Top {TOP_N} de {COL_CATEGORIA} por número de registros")
    plt.xlabel("Registros")
    _guardar("01_ranking_categorias.png")

    # 2. Distribución de la variable numérica
    plt.figure(figsize=(9, 5))
    plt.hist(res["muestra"].dropna(), bins=40)
    plt.title(f"Distribución de {COL_NUMERICA}")
    plt.xlabel(COL_NUMERICA)
    plt.ylabel("Frecuencia")
    _guardar("02_distribucion_numerica.png")

    # 3. Serie mensual
    if "mensual" in res:
        m = res["mensual"].copy()
        m["periodo"] = (m["anio"].astype(int).astype(str) + "-"
                        + m["mes"].astype(int).astype(str).str.zfill(2))
        plt.figure(figsize=(10, 5))
        plt.plot(m["periodo"], m["registros"], marker="o")
        plt.xticks(rotation=90)
        plt.title("Registros por mes")
        _guardar("03_serie_mensual.png")
        m.to_csv(f"{OUTPUT_DIR}/resultado_mensual.csv", index=False)

    # 4. Grupos de la variable numérica
    if "grupos" in res:
        g = res["grupos"]
        plt.figure(figsize=(8, 5))
        plt.bar(g["grupo_numerica"].astype(str), g["count"])
        plt.title(f"Registros por grupo de {COL_NUMERICA}")
        _guardar("04_grupos.png")

    # 5. Columnas adicionales
    for i, (c, t) in enumerate(res["extra"].items(), start=5):
        plt.figure(figsize=(8, 5))
        plt.bar(t[c].astype(str), t["count"])
        plt.title(f"Registros por {c}")
        plt.xticks(rotation=20, ha="right")
        _guardar(f"0{i}_{c}.png")
        t.to_csv(f"{OUTPUT_DIR}/resultado_{c}.csv", index=False)

    res["ranking"].to_csv(f"{OUTPUT_DIR}/resultado_ranking.csv", index=False)
    print(f"\n[VISUALIZACIÓN] Gráficas y tablas guardadas en '{OUTPUT_DIR}/'")


def main():
    spark = (SparkSession.builder
             .appName("AnalisisBatch_Tarea3")
             .master("local[*]")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    descargar_datos()
    df = cargar(spark)
    validar_columnas(df)
    df = limpiar(df)
    res = eda(df)
    visualizar(res)

    spark.stop()


if __name__ == "__main__":
    main()
