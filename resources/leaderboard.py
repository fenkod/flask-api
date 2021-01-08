from flask_restful import Resource
from helpers import get_connection
import json as json
import pandas as pd

class Leaderboard(Resource):
    def get(self, query_type):
        result = self.fetch_data(query_type)
        json_response = json.loads(result.to_json(orient='records', date_format='iso'))
        return (json_response)

    def fetch_data(self, query_type):
        db_connection = get_connection()
        cursor = db_connection.cursor()
        query = f"select '{query_type}' as leaderboard, 'current' as season;"
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