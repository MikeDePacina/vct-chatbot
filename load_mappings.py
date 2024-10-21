import json
import psycopg2
import os

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

# Open and read the JSON file
def load_json(conn, json_file, table_name, table_fields):
    cursor = conn.cursor()
    with open(json_file, 'r') as file:
        data = json.load(file)
        values = ""
        for name, id in data.items():
            values += f"('{id}', '{name}'),"
        query = f"INSERT INTO {table_name} ({table_fields}) VALUES {values[:-1]}"
        cursor.execute(query)
        conn.commit()
    cursor.close()

# load_json(conn, "./mappings/agents.json", "dim_agents", "agent_id, agent_name")
# load_json(conn, "./mappings/weapons.json", "dim_weapons", "weapon_id, weapon_name")

load_json(conn, "./mappings/maps.json", "dim_maps", "map_name, map_id")

conn.close()

