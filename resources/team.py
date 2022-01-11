from flask import current_app
from flask_restful import Resource
from helpers import fetch_dataframe, var_dump
from cache import cache_timeout, cache_invalidate_hour
import json as json
import pandas as pd

##
# This is the flask_restful Resource Class for the player API.
# Current Enpoint Structure:
# `/player/${query_type}/${team_id}`
# @param ${query_type}: ('bio'|'stats'|'gamelogs'|'positions'|'repertoire'|'abilities'|'locations'|'locationlogs'|'career'|'')
# @param ${team_id}: ([0-9]*|'All')
##
class Team(Resource):
    def __init__(self):
        self.team_id = 'NA'

    def get(self, query_type='NA', team_id='NA'):
        if (type(team_id) is int):
            self.team_id = int(team_id)
        return self.fetch_result(query_type, team_id)

    
    def fetch_result(self, query_type, team_id):
        # Caching wrapper for fetch_data
        result = None

        if (current_app.config.get('BYPASS_CACHE')):
            # Bypassing Caching of JSON Results
            result = self.fetch_data(query_type, team_id)
        else:
            # Using Cache for JSON Results
            cache_key_team_id = team_id
            cache_key_resource_type = self.__class__.__name__
            if (team_id == 'NA'):
                cache_key_team_id = 'all'

            cache_key = f'{cache_key_resource_type}-{query_type}-{cache_key_team_id}'
            result = current_app.cache.get(cache_key)
            if (result is None):
                result = self.fetch_data(query_type, team_id)
                current_app.cache.set(cache_key, result,cache_timeout(cache_invalidate_hour()))

        return result

    def fetch_data(self, query_type, team_id):
        query = self.get_query(query_type, team_id)
        query_var=None
        if (type(team_id) is int):
            query_var = team_id

        raw = fetch_dataframe(query, query_var)
        results = self.format_results(query_type, raw)
        output = self.get_json(query_type,team_id,results)

        return output

    def get_query(self, query_type, team_id):
        def default():
            return f"SELECT 'query not defined' AS error, '{query_type}' AS query, {team_id} AS id;"

        def roster():
            sql_query = ''

            table_select = (f'select 	players.mlb_player_id as mlbamid,'
                                        f'players.full_name,'
                                        f'case when players.primary_position in (\'SP\', \'RP\', \'P\') then 1 else 0 end as is_pitcher,'
                                        f'players.primary_position,'
                                        f'players.status,'
                                        f'last_game.game_date as last_game_date,'
                                        f'game_lineups."position" as last_game_position,'
                                        f'game_lineups."order" as last_game_order,'
                                        f'case when game_lineups.inning = 0 then true else false end as last_game_started,'
                                        f'projected_start.game_date as next_start,'
                                        f'highest_hitter_depth_chart_position.position as hitter_depth_chart_position,'
                                        f'highest_hitter_depth_chart_position.depth as hitter_depth_chart_depth,'
                                        f'highest_pitcher_depth_chart_position.position as pitcher_depth_chart_position,'
                                        f'highest_pitcher_depth_chart_position.depth as pitcher_depth_chart_depth,'
                                        f'teams.mlb_team_id as team_id,'
		                                f'teams.team_name '
                                f'from players '
                                f'inner join teams on teams.team_id = players.current_team_id '
                                f'left join lateral (select games.game_id, games.game_date from games where games.home_team_id = players.current_team_id or games.away_team_id = players.current_team_id order by games.game_date desc fetch first row only) last_game on true '
                                f'left join game_lineups on game_lineups.game_id = last_game.game_id and game_lineups.player_id = players.player_id '
                                f'left join lateral (select games.game_id, games.game_date from games where (games.home_projected_starting_pitcher_id = players.player_id or games.away_projected_starting_pitcher_id = players.player_id) and games.status = \'scheduled\' order by games.game_date fetch first row only) projected_start on true '
                                f'left join lateral (select * from depth_charts where depth_charts.player_id = players.player_id and depth_charts.team_id = teams.team_id and depth_charts."position" not in (\'SP\', \'BP\', \'CL\') order by depth_charts."depth" fetch first row only) highest_hitter_depth_chart_position on true '
                                f'left join lateral (select * from depth_charts where depth_charts.player_id = players.player_id and depth_charts.team_id = teams.team_id and depth_charts."position" in (\'SP\', \'BP\', \'CL\') order by depth_charts."depth" fetch first row only) highest_pitcher_depth_chart_position on true \n')
            team_select = ''

            if team_id != 'NA':
                team_select = 'where teams.mlb_team_id = %s'

            sql_query = table_select + team_select

            return sql_query
        queries = {
            "roster": roster
        }

        return queries.get(query_type, default)()

    def format_results(self, query_type, data):

        def default():
            return data

        formatting = {

        }

        return formatting.get(query_type, default)()
    
    def get_json(self, query_type, team_id, results):
        
        def default():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)
            
            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(results.to_json(orient='records', date_format='iso'))

        def roster():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results['last_game_date'] = pd.to_datetime(results['last_game_date']).dt.strftime("%a %m/%d/%Y")
            results['next_start'] = pd.to_datetime(results['next_start']).dt.strftime("%a %m/%d/%Y")
            results.fillna(value=json.dumps(None), inplace=True)

            records = []

            teamGroupings = results.groupby(['team_id', 'team_name'])
            for keys, teamValues in teamGroupings:
                teamRecord = {}
                teamRecord['team_id'] = int(keys[0])
                teamRecord['team_name'] = keys[1]
                del teamValues['team_id']
                del teamValues['team_name']
                teamRecord['players'] = json.loads(teamValues.sort_values(by='full_name').to_json(orient='records'))
                records.append(teamRecord)

            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(json.dumps(records))



        json_data = {
            "roster": roster
        }

        return json_data.get(query_type, default)()
