from flask_restful import Resource
from helpers import get_connection, cache_timeout
import json as json
import pandas as pd


class Player(Resource):
    def get(self, query_type, player_id):
        result = self.fetch_data(query_type, player_id)
        json_response = json.loads(result.to_json(orient='records', date_format='iso'))
        return (json_response)

    def fetch_data(self, query_type, player_id):
        db_connection = get_connection()
        cursor = db_connection.cursor()
        
        query = self.get_query(query_type, player_id)
        cursor_list = list()

        try:
            cursor.execute(query, cursor_list)
        except Exception:
            raise
        else:
            rows = cursor.fetchall()

        colnames = [desc[0] for desc in cursor.description]
        raw = pd.DataFrame(rows, columns=colnames)
        return raw

    def get_query(self, query_type, player_id):
        def default():
            return f"SELECT 'query not defined' AS error, '{query_type}' AS query, {player_id} AS id;"

        def repertoire():
            return (
                f'SELECT pitchermlbamid AS "id",'
                    f'pitchtype AS "pitch",' 
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
            "repertoire": repertoire
        }

        return queries.get(query_type, default)()