from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, split, explode, log
from datetime import datetime
import logging

logging.basicConfig(
    filename="logs/processor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("Debut processing")
# session spark
spark = SparkSession.builder \
    .appName("TMDB Processor") \
    .getOrCreate()

# chemin RAW (on lit tout)
input_path = "data/raw/*/*/*"

df = spark.read.parquet(input_path)

print("Data RAW chargée")

# =========================
#  VALIDATION (OBLIGATOIRE)
# =========================

df_clean = df.filter(
    (col("title").isNotNull()) &
    (col("vote_average") > 0) &
    (col("vote_count") > 0) &
    (col("release_date").isNotNull()) &
    (col("genres").isNotNull())
)
logging.info("Validation OK")
print("Validation OK")

# =========================
#  TRANSFORMATIONS
# =========================

# année
df_clean = df_clean.withColumn("year", year(col("release_date")))

# split genres (string → array)
df_clean = df_clean.withColumn("genre_list", split(col("genres"), "\\|"))

# explode genres (1 ligne par genre)
df_clean = df_clean.withColumn("genre", explode(col("genre_list")))

# score performance
df_clean = df_clean.withColumn(
    "performance_score",
    col("vote_average") * log(col("vote_count") + 1)
)
logging.info("Transformation OK")
print("Transformation OK")

# =========================
#  ÉCRITURE SILVER
# =========================

today = datetime.now()
year_p = today.strftime("%Y")
month_p = today.strftime("%m")
day_p = today.strftime("%d")

output_path = f"data/silver/year={year_p}/month={month_p}/day={day_p}"

df_clean.write.mode("overwrite").parquet(output_path)
logging.info("Ecriture SILVER OK")
print("Donnees ecrites dans SILVER")