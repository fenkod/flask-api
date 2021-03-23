from flask import current_app
from flask_restful import Resource
from helpers import fetch_dataframe, date_validate, var_dump
from cache import cache_timeout, cache_invalidate_hour
import json as json
import pandas as pd

##
# This is the flask_restful Resource Class for the SP Roundup and Batterbox API.
# It accepts both the batterbox and roundup endpoints 
# Current Enpoint Structure:
# `/roundup/${player_type}/${day}` - Hitters and Pitchers
# `/batterbox/${day}` - Hitters only
# @param ${player_type}: ('hitter'|'pitcher')
# @param ${day}: ([0-9]2/[0-9]2/[0-9]4|'latest')
##
class Roundup(Resource):
    def __init__(self):
        self.day = 'latest'
        self.player_type = 'pitcher'

    def get(self, player_type='pitcher', day='latest'):
        if (day != 'latest' and (not date_validate(day))):
            day = 'latest'
        
        if (player_type == 'roundup'):
            player_type = 'pitcher'
        elif (player_type == 'battersbox'):
            player_type == 'hitter'
        
        if (player_type != 'pitcher' and player_type != 'hitter'):
            player_type = 'pitcher'

        self.day = day
        self.player_type = player_type
        
        return self.fetch_result(player_type, day)

    
    def fetch_result(self, player_type, day):
        # Caching wrapper for fetch_data
        result = None

        if (current_app.config.get('BYPASS_CACHE')):
            print('Bypassing Caching of JSON Results')
            result = self.fetch_data(player_type, day)
        else:
            print('Using Cache for JSON Results')
            cache_key_resource_type = self.__class__.__name__

            cache_key = f'{cache_key_resource_type}-{player_type}-{day}'
            result = current_app.cache.get(cache_key)
            if (result is None):
                result = self.fetch_data(player_type, day)
                current_app.cache.set(cache_key, result,cache_timeout(cache_invalidate_hour()))

        return result

    def fetch_data(self, player_type, day):
        query = self.get_query(player_type, day)

        raw = fetch_dataframe(query,day)
        results = self.format_results(player_type, raw)
        output = self.get_json(player_type,day,results)

        return output

    def get_query(self, player_type, day):
        def default():
            return f"SELECT 'query not defined' AS error, '{player_type}' AS player_type, {day} AS day;"

        def hitter():
            return (
                f'SELECT h.gamepk AS "game_pk,"'
                    f'h.ghuid,'
                    f'hittermlbamid AS "player_id",'
                    f'hittername AS "playername",'
                    f'num_pa AS "pa",'
                    f'num_ab AS "ab",'
                    f'num_hit AS "hits",'
                    f'(num_hit - num_2b - num_3b - num_hr) AS "1b",'
                    f'num_2b AS "2b",'
                    f'num_3b AS "3b",'
                    f'num_hr AS "hr",'
                    f'num_runs AS "r",'
                    f'num_rbi AS "rbi",'
                    f'num_k AS "k",'
                    f'num_bb AS "bb",'
                    f'num_ibb AS "ibb",'
                    f'num_hbp AS "hbp",'
                    f'num_sb AS "sb",'
                    f'num_cs AS "cs",'
                    f'park,'
                    f'CASE '
                        f"WHEN park = 'HOME' THEN t_home.abbreviation "
                        f"WHEN park = 'AWAY' THEN t_away.abbreviation "
                        f"ELSE 'Unknown' "
                        f'END AS "team",'
                    f'CASE '
                        f"WHEN park = 'HOME' THEN t_away.abbreviation "
                        f"WHEN park = 'AWAY' THEN t_home.abbreviation "
                        f'END AS "opponent" '
                f'FROM statcast_hitters h '
                f'JOIN statsapi_schedule ss ON h.gamepk = ss.gamepk '
                f'JOIN teams t_away ON ss.teams_away_team_id = t_away.mlb_id '
                f'JOIN teams t_home ON ss.teams_home_team_id = t_home.mlb_id '
                f"WHERE game_date = '2019-06-23';"
            )

        def pitcher():
            return (
                f'SELECT p.gamepk AS "game_pk",'
                    f'p.ghuid,'
                    f'pitchermlbamid AS "player_id",'
                    f'pitchername AS "playername",'
                    f'num_ip AS "ip",'
                    f'num_earned_runs AS "er",'
                    f'num_hits AS "hits",'
                    f'num_k AS "k",'
                    f'num_bb AS "bb",'
                    f'num_pitches AS "pitch-count",'
                    f'num_whiff AS "whiff",'
                    f'csw_pct AS "csw_pct",'
                    f'park,'
                    f'CASE '
                        f"WHEN win = 1 THEN 'W' "
                        f"WHEN loss = 1 THEN 'L' "
                        f"ELSE 'ND' "
                        f'END AS "decision", '
                    f'CASE '
                        f"WHEN park = 'HOME' THEN t_home.abbreviation "
                        f"WHEN park = 'AWAY' THEN t_away.abbreviation "
                        f"ELSE 'Unknown' "
                        f'END AS "team", '
                    f'CASE '
                        f"WHEN park = 'HOME' THEN t_away.abbreviation "
                        f"WHEN park = 'AWAY' THEN t_home.abbreviation "
                        f'END AS "opponent" '
                f'FROM statcast_pitchers p '
                f'JOIN statsapi_schedule ss ON p.gamepk = ss.gamepk '
                f'JOIN teams t_away ON ss.teams_away_team_id = t_away.mlb_id '
                f'JOIN teams t_home ON ss.teams_home_team_id = t_home.mlb_id '
                f"WHERE game_date = '2019-06-23' "
                f'AND start = 1;'
            )

        queries = {
            "hitter": hitter,
            "pitcher": pitcher
        }

        return queries.get(player_type, default)()

    def format_results(self, player_type, data):

        def default():
            return data

        def roundup():
            return data

        formatting = {
            "hitter": roundup,
            "pitcher": roundup
        }

        return formatting.get(player_type, default)()
    
    def get_json(self, player_type, day, results):
        
        def default():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)
            
            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(results.to_json(orient='records', date_format='iso'))
        
        def roundup():
            results.fillna(value=0, inplace=True)
            records = json.loads(results.to_json(orient='records'))

            output = []
            # Keep game data on top level and move all stats to its own object. Allows us to use for pitchers and hitters without needing to change code.
            for value in records:
                data_struct = { 
                    "player_id": value["player_id"], 
                    "team": value["team"],
                    "playername": value["playername"],
                    "park": value["park"],
                    "opponent": value["opponent"],
                    "game_pk": value["game_pk"],
                    "ghuid": value["ghuid"],
                }
                del value['player_id']
                del value['team']
                del value['playername']
                del value['park']
                del value['opponent']
                del value['game_pk']
                del value['ghuid']

                data_struct['stats'] = value
                output.append(data_struct)

            return output

        json_data = {
            "hitter": roundup,
            "pitcher": roundup
        }

        return json_data.get(player_type, default)()

