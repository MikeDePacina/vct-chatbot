import json
import pandas as pd

json_file = "./mappings/mapping_data_intl.json"

tournament_id = "108871629797692793"

column_names_team_mapping = [
    "platformGame_id",
    "team_id",
    "team_num"
]

column_names_player_mapping = [
    "platformGame_id",
    "player_id",
    "player_num"
]

df_champions_2022_team_mappings = pd.DataFrame(columns=column_names_team_mapping)
df_champions_2022_player_mappings = pd.DataFrame(columns=column_names_player_mapping)

with open(json_file, 'r') as file:
    json_object = json.load(file)

    for match in json_object:
        if match['tournamentId'] == tournament_id:
            platformGame_id = match['platformGameId']
            for key, value in match['participantMapping'].items():
                new_row = pd.DataFrame([{
                    "platformGame_id": platformGame_id,
                    "player_id": value,
                    "player_num": key
                }])
                df_champions_2022_player_mappings = pd.concat([df_champions_2022_player_mappings, new_row], ignore_index=True)
            
            for key, value in match['teamMapping'].items():
                new_row = pd.DataFrame([{
                    "platformGame_id": platformGame_id,
                    "team_id": value,
                    "team_num": key
                }])
                df_champions_2022_team_mappings = pd.concat([df_champions_2022_team_mappings, new_row], ignore_index=True)


df_champions_2022_team_mappings.to_csv("champions_2022_team_mappings.csv", index=False)            
df_champions_2022_player_mappings.to_csv("champions_2022_player_mappings.csv", index=False)            
