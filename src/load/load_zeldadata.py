import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_join 


spark = SparkSession.builder \
    .appName("ZeldaDataToSupabase") \
    .config("spark.jars", "postgresql-42.7.3.jar") \
    .getOrCreate()


JDBC_URL = "jdbc:postgresql://<SEU_HOST>:5432/<SEU_BANCO>"
JDBC_PROPERTIES = {
    "user": "<SEU_USUARIO>",
    "password": "<SUA_SENHA>",
    "driver": "org.postgresql.Driver",

}

BASE_PROCESSED_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",                                    
    "transform",                            
    "data",                               
    "processed"                             
)

def read_parquet_data(entity_name):
    """Lê um diretório Parquet processado pelo Spark."""
    parquet_directory_path = os.path.join(BASE_PROCESSED_PATH, entity_name)
    print(f"Lendo dados de: {parquet_directory_path}")
    return spark.read.parquet(parquet_directory_path)

def load_to_postgresql(dataframe, table_name):
    """Carrega um DataFrame Spark para uma tabela PostgreSQL."""
    print(f"Carregando dados para a tabela '{table_name}' no PostgreSQL...")
    dataframe.write.jdbc(
        url=JDBC_URL,
        table=table_name,
        mode="overwrite", 
        properties=JDBC_PROPERTIES
    )
    print(f"✅ Dados da tabela '{table_name}' carregados com sucesso para o PostgreSQL.")

if __name__ == "__main__":
    try:

        characters_df = read_parquet_data("characters")

        if "appearances" in characters_df.columns:
            characters_df = characters_df.withColumn("appearances", array_join(col("appearances"), ", "))
        else:
            print("Aviso: 'appearances' não encontrada em characters_df. Pulando transformação.")

        load_to_postgresql(characters_df, "characters")

        games_df = read_parquet_data("games")

        load_to_postgresql(games_df, "games")

        dungeons_df = read_parquet_data("dungeons")

        load_to_postgresql(dungeons_df, "dungeons")

        monsters_df = read_parquet_data("monsters")

        load_to_postgresql(monsters_df, "monsters")

    except Exception as e:
        print(f"❌ Ocorreu um erro fatal durante o carregamento de dados: {e}")

    finally:
        spark.stop()
        print("Sessão Spark encerrada.")