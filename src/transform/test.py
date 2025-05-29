import duckdb
import os 
PROCESSED_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "processed")
games_parquet_path = os.path.join(PROCESSED_PATH, "games") 

conn = duckdb.connect()

result = conn.execute(f"SELECT name, released_date FROM read_parquet('{games_parquet_path}/*') WHERE released_date > '2010-01-01'").fetchdf()


print(result)