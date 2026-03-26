from pyspark.sql import SparkSession
from datetime import datetime
import logging

logging.basicConfig(
    filename="logs/feeder.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("Debut ingestion RAW")
# créer session spark
spark = SparkSession.builder \
    .appName("TMDB Feeder") \
    .getOrCreate()

# chemin dataset
input_path = "data/input/tmdb_movies_2021_2025.parquet"

# lire données
df = spark.read.parquet(input_path)
logging.info("Lecture des donnees OK")
# date ingestion
today = datetime.now()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")

# output path (partitionné)
output_path = f"data/raw/year={year}/month={month}/day={day}"

# écriture
df.write.mode("overwrite").parquet(output_path)
logging.info("Donnees ecrites dans RAW")
print("Données ingérées dans RAW")