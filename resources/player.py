from flask import current_app
from flask_restful import Resource
from helpers import get_connection, create_player_query, create_player_positions_query, var_dump
from cache import cache_timeout, cache_invalidate_hour
import json as json
import pandas as pd


class Player(Resource):
    def get(self, query_type='NA', player_id='NA'):
        # We can have an empty query_type or player_id which return the collections of stats.
        if (query_type == 'NA' and (player_id == 'NA' or type(player_id) is int)):
            query_type = 'stats'
        elif (player_id == 'NA' and query_type.isnumeric()):
            player_id = int(query_type)
            query_type = 'stats'

        cache_key_player_id = player_id
        cache_key_resource_type = self.__class__.__name__
        if (player_id == 'NA'):
            cache_key_player_id = 'all'

        cache_key = f'{cache_key_resource_type}-{query_type}-{cache_key_player_id}'
        result = current_app.cache.get(cache_key)
        var_dump(result)
        if (result is None):
            result = self.fetch_data(query_type, player_id)
            current_app.cache.set(cache_key, result,cache_timeout(cache_invalidate_hour))

        return (result)

    def fetch_data(self, query_type, player_id):
        db_connection = get_connection()
        cursor = db_connection.cursor()
        
        query = self.get_query(query_type, player_id)
        cursor_list = list()
        if (type(player_id) is int):
            cursor_list.append(player_id)

        try:
            cursor.execute(query, cursor_list)
        except Exception:
            raise
        else:
            rows = cursor.fetchall()

        colnames = [desc[0] for desc in cursor.description]
        raw = pd.DataFrame(rows, columns=colnames)
        results = self.format_results(query_type, raw)
        output = self.get_json(query_type,player_id,results)

        return output

    def get_query(self, query_type, player_id):
        def default():
            return f"SELECT 'query not defined' AS error, '{query_type}' AS query, {player_id} AS id;"
        
        def stats():
            return create_player_query(player_id)

        def positions():
            return create_player_positions_query(player_id)

        def repertoire():
            return (
                f'SELECT pitchtype AS "pitch",' 
                    f'year_played AS "year",' 
                    f'opponent_handedness AS "split",'
                    f'usage_pct AS "usage",'
                    f'batting_average AS "avg",' 
                    f'o_swing_pct AS "o-swing",'
                    f'zone_pct AS "zone",'
                    f'swinging_strike_pct AS "swinging-strike",'
                    f'called_strike_pct AS "called-strike",'
                    f'csw_pct AS "csw" from player_page_repertoire '
                f"WHERE pitchermlbamid = {player_id} AND home_away = 'All' "
                f'ORDER BY pitchtype, year_played, opponent_handedness;'
            )
        
        queries = {
            "repertoire": repertoire,
            "stats": stats,
            "positions": positions
        }

        return queries.get(query_type, default)()

    def format_results(self, query_type, data):

        def default():
            return data

        def repertoire():
            data['year'] = pd.to_numeric(data['year'], downcast='integer')
            data[['usage','avg','o-swing','zone','swinging-strike','called-strike','csw']] = data[['usage','avg','o-swing','zone','swinging-strike','called-strike','csw']].apply(pd.to_numeric)
            formatted_data = data.set_index(['pitch','year','split'])
            
            return formatted_data

        formatting = {
           "repertoire": repertoire 
        }

        return formatting.get(query_type, default)()
    
    def get_json(self, query_type, player_id, results):
        
        def default():
            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(results.to_json(orient='records', date_format='iso'))

        def repertoire():
            # Sort our DataFrame so we have a prettier JSON format for the API
            result_dict = results.to_dict(orient='index')
            output_dict = { 'player_id': player_id, query_type: {'pitches':{}} }

            # Make sure our index keys exist in our dict structure then push on our data values
            for key, value in result_dict.items():
                
                pitch_key = key[0]
                if pitch_key not in output_dict[query_type]['pitches']:
                    output_dict[query_type]['pitches'][pitch_key] = {'years':{}}

                year_key = key[1]
                if year_key not in output_dict[query_type]['pitches'][pitch_key]['years']:
                    output_dict[query_type]['pitches'][pitch_key]['years'][year_key] = {}
                
                split_key = key[2]
                output_dict[query_type]['pitches'][pitch_key]['years'][year_key][split_key] = value
            
            return output_dict

        json_data = {
            "repertoire": repertoire 
        }

        return json_data.get(query_type, default)()

