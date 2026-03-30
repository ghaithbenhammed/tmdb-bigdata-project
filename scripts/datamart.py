from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
from datetime import datetime
import logging

# =========================
# LOGGING
# =========================
logging.basicConfig(
    filename="logs/datamart.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Debut GOLD")

# =========================
# SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("TMDB Datamart") \
    .getOrCreate()

# =========================
# LECTURE SILVER
# =========================
input_path = "data/silver/year=*/month=*/day=*"
df = spark.read.parquet(input_path)

print("Data SILVER chargee")
logging.info("Lecture SILVER OK")
# =========================
# TOP GENRES
# =========================

df_top_genres = df.groupBy("genre").agg(
    avg("performance_score").alias("avg_score"),
    count("*").alias("nb_films")
)

# trier par performance
df_top_genres = df_top_genres.orderBy(col("avg_score").desc())

print("Top genres calcules")
logging.info("Top genres OK")
# =========================
# DATE
# =========================

today = datetime.now()
year_p = today.strftime("%Y")
month_p = today.strftime("%m")
day_p = today.strftime("%d")

# =========================
# WRITE TOP GENRES
# =========================

output_genres = f"data/gold/top_genres/year={year_p}/month={month_p}/day={day_p}"

df_top_genres.write.mode("overwrite").parquet(output_genres)

print("Top genres ecrits")
logging.info("Top genres ecrits")
# =========================
# TOP MOVIES GLOBAL
# =========================

df_top_movies_global = df.orderBy(col("performance_score").desc())

# garder top 20
df_top_movies_global = df_top_movies_global.limit(20)

print("Top movies global calcules")
logging.info("Top movies global OK")
# =========================
# WRITE TOP MOVIES GLOBAL
# =========================

output_movies = f"data/gold/top_movies_global/year={year_p}/month={month_p}/day={day_p}"

df_top_movies_global.write.mode("overwrite").parquet(output_movies)

print("Top movies global ecrits")
logging.info("Top movies global ecrits")
# =========================
# STATS PAR ANNEE
# =========================

df_stats_year = df.groupBy("year").agg(
    count("*").alias("nb_films"),
    avg("performance_score").alias("avg_score"),
    avg("vote_average").alias("avg_rating")
)

# trier par année
df_stats_year = df_stats_year.orderBy("year")

print("Stats par annee calculees")
logging.info("Stats year OK")
# =========================
# WRITE STATS YEAR
# =========================

output_stats = f"data/gold/stats_year/year={year_p}/month={month_p}/day={day_p}"

df_stats_year.write.mode("overwrite").parquet(output_stats)

print("Stats year ecrites")
logging.info("Stats year ecrites")