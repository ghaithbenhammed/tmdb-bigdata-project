from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, split, explode, log, substring
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from datetime import datetime
import logging

# =========================
# LOGGING
# =========================
logging.basicConfig(
    filename="logs/processor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Debut processing")

# =========================
# SESSION SPARK
# =========================
spark = SparkSession.builder \
    .appName("TMDB Processor") \
    .getOrCreate()

# =========================
# LECTURE RAW
# =========================
input_path = "data/raw/*/*/*"
df = spark.read.parquet(input_path)

print("Data RAW chargee")
logging.info("Lecture RAW OK")

# =========================
# LECTURE JSON 
# =========================
json_path = "data/input/dataset-metadata.json"
df_json = spark.read.json(json_path)

print("JSON charge")
logging.info("JSON charge")

# =========================
# TRANSFORMATION JSON
# =========================
df_json_clean = df_json.select(explode("done_months").alias("date_range"))

df_json_clean = df_json_clean.withColumn(
    "year_json",
    substring("date_range", 1, 4)
)

print("JSON transforme")
logging.info("JSON transforme")

# =========================
# VALIDATION
# =========================
df_clean = df.filter(
    (col("title").isNotNull()) &
    (col("vote_average") > 0) &
    (col("vote_count") > 0) &
    (col("release_date").isNotNull()) &
    (col("genres").isNotNull())
)

print("Validation OK")
logging.info("Validation OK")

# =========================
# TRANSFORMATIONS
# =========================

# année
df_clean = df_clean.withColumn("year", year(col("release_date")))

# split genres
df_clean = df_clean.withColumn("genre_list", split(col("genres"), "\\|"))

# explode genres
df_clean = df_clean.withColumn("genre", explode(col("genre_list")))

# score performance
df_clean = df_clean.withColumn(
    "performance_score",
    col("vote_average") * log(col("vote_count") + 1)
)

print("Transformation OK")
logging.info("Transformation OK")

# =========================
# JOINTURE 
# =========================
df_joined = df_clean.join(
    df_json_clean,
    df_clean.year == df_json_clean.year_json,
    "left"
)

print("Jointure OK")
logging.info("Jointure OK")

# =========================
# DATE
# =========================
today = datetime.now()
year_p = today.strftime("%Y")
month_p = today.strftime("%m")
day_p = today.strftime("%d")

# =========================
# WINDOW FUNCTION (TOP FILMS)
# =========================
window_spec = Window.partitionBy("year").orderBy(col("performance_score").desc())

df_top_movies = df_joined.withColumn(
    "rank",
    row_number().over(window_spec)
)

# top 10 par année
df_top_movies = df_top_movies.filter(col("rank") <= 10)

print("Top films calcules")
logging.info("Top films calcules")

# =========================
# ECRITURE TOP MOVIES
# =========================
output_top = f"data/silver/top_movies/year={year_p}/month={month_p}/day={day_p}"

df_top_movies.write.mode("overwrite").parquet(output_top)

print("Top movies ecrits")
logging.info("Top movies ecrits")

# =========================
# ECRITURE SILVER
# =========================
output_path = f"data/silver/year={year_p}/month={month_p}/day={day_p}"

df_joined.write.mode("overwrite").parquet(output_path)

print("Donnees ecrites dans SILVER")
logging.info("Ecriture SILVER OK")