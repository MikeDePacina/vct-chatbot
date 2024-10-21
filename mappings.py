import json
import os
import pandas as pd

teams_file = f"{os.getcwd()}\mappings\\teams.json"
players_file = f"{os.getcwd()}\mappings\\players.json"
tournaments_file = f"{os.getcwd()}\mappings\\tournaments.json"
mapping_data_file = f"{os.getcwd()}\mappings\\mapping_data.json"
game_logs_file = f"{os.getcwd()}\mappings\\val_53e91649-b291-49ce-adc2-4095b71a4e21.json"

def make_teams_df():
   df = pd.read_json(teams_file)
   return df

def make_players_df():
   df = pd.read_json(players_file)
   return df

# def make_tournaments_df():
#    df = pd.read_json(tournaments_file)
#    return df

# def make_game_mapping_data_df():
#    df = pd.read_json(mapping_data_file)
#    return df



df_teams = make_teams_df()
df_players = make_players_df()
# df_tournaments = make_tournaments_df()
# df_game_mapping = make_game_mapping_data_df()
# df_game_logs = game_logs_df()

print(df_teams[:5])
# print(df_teams.columns)
print(df_players[:5])
# print(df_players.columns)
# print(df_tournaments[:5])
# print(df_tournaments.columns)
# print(df_game_mapping[:5])
# print(df_game_mapping.columns)

df_teams.to_csv("teams_mapping.csv", index=False)
df_players.to_csv("players_mapping.csv", index=False)