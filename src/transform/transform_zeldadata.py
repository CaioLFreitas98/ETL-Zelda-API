from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode 
import os

spark = SparkSession.builder \
    .appName("ZeldaETL") \
    .config("spark.hadoop.validateOutputSpecs", "false") \
    .getOrCreate()

RAW_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "extract", "data", "raw"))
TRANSFORMED_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "processed"))

os.makedirs(TRANSFORMED_PATH, exist_ok=True)

def read_json(filename):
    return spark.read.option("multiLine", "true").json(os.path.join(RAW_PATH, filename))


def transform_characters():
    df = read_json("characters.json")

    print("Esquema original:")
    df.printSchema()

    df_exploded = df.select(explode(col("data")).alias("character_data"))

    df_clean = df_exploded.select(
        col("character_data.name").alias("name"),
        col("character_data.description").alias("description"),
        col("character_data.gender").alias("gender"),
        col("character_data.race").alias("race"),
        col("character_data.id").alias("id")
    ).dropDuplicates(["id"]).na.drop()

    df_clean.write.mode("overwrite").parquet(os.path.join(TRANSFORMED_PATH, "characters"))
    print("✅ characters processado e salvo.")

def transform_games():
    df = read_json("games.json")
    print("Esquema original (games):")
    df.printSchema()

    df_exploded = df.select(explode(col("data")).alias("game_data"))

    df_clean = df_exploded.select(
        col("game_data.id").alias("id"),
        col("game_data.name").alias("name"),
        col("game_data.released_date").alias("released_date")
    ).dropDuplicates().na.drop()

    df_clean.write.mode("overwrite").parquet(os.path.join(TRANSFORMED_PATH, "games"))
    print("✅ games processado e salvo.")

def transform_dungeons():
    df = read_json("dungeons.json")
    print("Esquema original (dungeons):")
    df.printSchema()


    df_exploded = df.select(explode(col("data")).alias("dungeon_data"))

    df_clean = df_exploded.select(
        col("dungeon_data.id").alias("id"),
        col("dungeon_data.name").alias("name"),
        col("dungeon_data.description").alias("description")

    ).dropDuplicates().na.drop()
    df_clean.write.mode("overwrite").parquet(os.path.join(TRANSFORMED_PATH, "dungeons"))
    print("✅ dungeons processado e salvo.")

def transform_monsters():
    df = read_json("monsters.json")
    print("Esquema original (monsters):") 

    df_exploded = df.select(explode(col("data")).alias("monster_data"))

    df_clean = df_exploded.select(
        col("monster_data.id").alias("id"),
        col("monster_data.name").alias("name"),
        col("monster_data.description").alias("description")
    ).dropDuplicates().na.drop()
    df_clean.write.mode("overwrite").parquet(os.path.join(TRANSFORMED_PATH, "monsters"))
    print("✅ monsters processado e salvo.")
    
if __name__ == "__main__":
    transform_characters()
    transform_games()
    transform_dungeons()
    transform_monsters()