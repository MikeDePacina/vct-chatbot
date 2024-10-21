import subprocess
import json
import pandas as pd

json_file = "val_c1514574-4ba4-4ad3-b5ef-dbfef9976946.json"

ROUND_DECIDED = "roundDecided"
PLAYER_DIED = "playerDied"
CONFIGURATION_EVENT = "configuration"
DAMAGE_EVENT = "damageEvent"
GAME_DECIDED = "gameDecided"

round_number = 1
map_guid = ""
map_displayName = ""


tournament_id = "108871629797692793"
match_id = "108871629798741439"

jq_command = f'jq -c "." {json_file}'

column_names_round_details = [
    "tournament_id",
    "match_id",
    "map_guid",
    # "map_displayName",
    "round_number",
    "attacking_team_id",
    "defending_team_id",
    "winning_team_id",
    "cause"
]

column_names_map_results = [
    "tournament_id",
    "match_id",
    "map_guid",
    "winning_team_id",
    "num_of_rounds",    
]

column_names_config =[
    "tournament_id",
    "platformGame_id",
    "match_id",
    "team_id",
    # "team_name",
    "player_id",
    # "player_displayName",
    "map_guid",
    "map_displayName",
    "selected_agent_guid"
]

column_names_player_died = [
    "tournament_id",
    "match_id",
    "map_guid",
    "round_number",
    "killer_id",
    "assistants_id",
    # "killer_name",
    "victim_id",
    # "victim_name",
    "weapon_guid",
    "ability_guid"
]

column_names_damage_event = [
    "tournament_id",
    "match_id",
    "map_guid",
    "round_number",
    "causer_id",
    # "causer_name",
    "victim_id",
    # "victim_name",
    "location",
    "damageAmount",
    "ledToKill"
]

player_ids = {
    "1": "106525489805459472",
    "2": "108273989413516929",
    "3": "108273958835992183",
    "4": "106525538484682825",
    "5": "106664993881319312",
    "6": "106230274512017155",
    "7": "106230271915475632",
    "8": "103537287230111095",
    "9": "107764993888872529",
    "10": "107729646065265994"
}

team_ids = {
    "16": "105680972836508184",
    "17": "105748037960121143"
}

df_damage_event = pd.DataFrame(columns=column_names_damage_event)
df_player_died = pd.DataFrame(columns=column_names_player_died)
df_config = pd.DataFrame(columns=column_names_config)
df_round_details = pd.DataFrame(columns=column_names_round_details)
df_map_results = pd.DataFrame(columns=column_names_map_results)

# Step 3: Read the JSON objects and append to the DataFrame
with subprocess.Popen(jq_command, shell=True, stdout=subprocess.PIPE) as proc:
    for test in proc.stdout:
        # Decode the line and convert it to a dictionary
        json_object = json.loads(test.decode())
        
            
        for line in json_object:
            
            if ROUND_DECIDED in line:
                round_number += 1
            
            if GAME_DECIDED in line:
                game_result = line.get(GAME_DECIDED)
                # print(game_result)
                for round in game_result.get("spikeMode").get("completedRounds"):
                    new_row = pd.DataFrame([{
                        "tournament_id": tournament_id,
                        "match_id": match_id,
                        "map_guid": map_guid,
                        "round_number": round.get("roundNumber"),
                        "attacking_team_id": round.get("spikeModeResult").get("attackingTeam").get("value"),
                        "defending_team_id": round.get("spikeModeResult").get("defendingTeam").get("value"),
                        "winning_team_id": round.get("winningTeam").get("value"),
                        "cause": round.get("spikeModeResult").get("cause")
                    }])
                    df_round_details = pd.concat([df_round_details, new_row], ignore_index=True)
                
                if len(df_map_results) == 0:
                    new_row = pd.DataFrame([{
                        "tournament_id": tournament_id,
                        "match_id": match_id,
                        "map_guid": map_guid,
                        "winning_team_id": game_result.get("winningTeam").get("value"),
                        "num_of_rounds": game_result.get("spikeMode").get("currentRound")
                    }])
                    df_map_results = pd.concat([df_map_results, new_row], ignore_index=True)

            if DAMAGE_EVENT in line:
                damage_event = line.get(DAMAGE_EVENT)
                new_row = pd.DataFrame([{
                    "tournament_id": tournament_id,
                    "match_id": match_id,
                    "map_guid": map_guid,
                    "round_number": round_number,
                    "causer_id": damage_event.get("causerId").get("value"),
                    "victim_id": damage_event.get("victimId").get("value"),
                    "location": damage_event.get("location"),
                    "damageAmount": damage_event.get("damageAmount"),
                    "ledToKill": damage_event.get("killEvent")
                }])
                df_damage_event = pd.concat([df_damage_event, new_row], ignore_index=True)

            if PLAYER_DIED in line:
                dead_player = line.get(PLAYER_DIED)
                assists = []
                for assistant in dead_player.get("assistants"):
                    assists.append(assistant.get("assistantId").get("value"))
                # print(dead_player)
                new_row = pd.DataFrame([{ 
                    "tournament_id": tournament_id,
                    "match_id": match_id,
                    "map_guid": map_guid,
                    "round_number": round_number,
                    "killer_id": dead_player.get("killerId").get("value"),
                    "assistants_id": assists,
                    "victim_id": dead_player.get("deceasedId").get("value"),
                    "weapon_guid": dead_player.get("weapon").get("fallback").get("guid") if dead_player.get("weapon") else None,
                    "ability_guid": dead_player.get("ability").get("fallback").get("guid") if dead_player.get("ability") else None
                }])

                df_player_died = pd.concat([df_player_died, new_row], ignore_index=True)
            # print(line)
            # Check if the object has the required fields
            if CONFIGURATION_EVENT in line:
                if len(df_config) == 10:
                    continue
                players_team_map = {}
                config = line.get(CONFIGURATION_EVENT)
                platformGame_id = line.get("platformGameId")
                for team in config.get("teams"):
                    for player in team.get("playersInTeam"):
                        players_team_map[player.get("value")] = team.get("teamId").get("value")

                map_guid = config.get("selectedMap").get("fallback").get("guid")
                map_displayName = config.get("selectedMap").get("fallback").get("displayName")
                
                for player in config.get("players"):
                    player_id = player.get("playerId").get("value")
                    new_row = pd.DataFrame([{
                        "tournament_id": tournament_id,
                        "platformGame_id": platformGame_id,
                        "match_id": match_id,
                        "team_id": players_team_map[player_id],
                        "player_id": player_id,
                        "map_guid": map_guid,
                        "map_displayName": map_displayName,
                        "selected_agent_guid": player.get("selectedAgent").get("fallback").get("guid")
                    }])
                    
                    df_config = pd.concat([df_config, new_row], ignore_index=True)
                    
# print(df_config)
# print(df_player_died)
# print(df_round_details)
# print(df_map_results)  
# print(df_damage_event)
# print(df_player_died.dtypes)                  
# df_player_died["victim_id"] = df_player_died["victim_id"].astype(int)
# df_player_died.to_csv("108871629798741439_killing_stats.csv", index=False)
# df_round_details.to_csv("108871629798741439_rounds_details.csv", index=False)
# df_map_results.to_csv("108871629798741439_map_results.csv", index=False)
df_config.to_csv("108871629798741439_match_config.csv", index=False)     
# df_damage_event.to_csv("108871629798741439_damage_events.csv", index=False)