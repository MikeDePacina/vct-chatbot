import psycopg2
import json
import os
from dotenv import load_dotenv

load_dotenv()


player_mapping = "./mappings/players.json"
team_mapping = "./mappings/teams.json"

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

def load_to_table(conn, table_name, json_file, table_fields, json_fields):
    cursor = conn.cursor()
    batch_size = 10
    values = ""
    fields = "".join([f"{field}," for field in table_fields])[:-1]
    with open(json_file, 'r', encoding="utf-8") as file:
        json_object = json.load(file)
        
        for idx, object in enumerate(json_object):
            row = ""
            for field in json_fields:
                row += f"'{object[field]}',"
            values += f"({row[:-1]})," #-1 to exclude the last comma
            row = ""
            
            if idx % batch_size == 0:
                #-1 to exclude the last comma
                #ON CONFLICT DO NOTHING to avoid duplicates/same person can have updates to profile so can have
                #multiple entries
                query = f"INSERT INTO {table_name} ({fields}) VALUES {values[:-1]} ON CONFLICT ({table_fields[0]}) DO NOTHING;" 
                cursor.execute(query)
                conn.commit()
                values = ""
        if values:
            query = f"INSERT INTO {table_name} ({fields}) VALUES {values[:-1]} ON CONFLICT ({table_fields[0]}) DO NOTHING;"
            cursor.execute(query)
            conn.commit()

        cursor.close()
      

# load_to_table(conn, "dim_players", player_mapping, ["player_id", "player_handle"], ["id", "handle"])
load_to_table(conn, "dim_teams", team_mapping, ["team_id", "team_name", "light_logo_url"], ["id", "name", "light_logo_url"])

conn.close()