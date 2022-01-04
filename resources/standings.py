from flask import current_app
from flask_restful import Resource
from numpy.core.records import record
from helpers import fetch_dataframe, var_dump
from cache import cache_timeout, cache_invalidate_hour
from datetime import date, datetime
import json as json
import pandas as pd

##
# This is the flask_restful Resource Class for the player API.
# Current Enpoint Structure:
# `/player/${query_type}/${player_id}`
# @param ${query_type}: ('bio'|'stats'|'gamelogs'|'positions'|'repertoire'|'abilities'|'locations'|'locationlogs'|'career'|'')
# @param ${player_id}: ([0-9]*|'All')
##
class Standings(Resource):
    def __init__(self):
        self.query_year = None

    def get(self, query_type='NA', query_year='NA'):
        if (query_type == 'NA'):
            query_type = 'division'
        
        return self.fetch_result(query_type, query_year)

    
    def fetch_result(self, query_type, query_year):
        # Caching wrapper for fetch_data
        result = None

        if (current_app.config.get('BYPASS_CACHE')):
            # Bypassing Caching of JSON Results
            result = self.fetch_data(query_type, query_year)
        else:
            # Using Cache for JSON Results
            cache_key_year = query_year
            cache_key_resource_type = self.__class__.__name__
            if (query_year == 'NA'):
                cache_key_year = 'all'

            cache_key = f'{cache_key_resource_type}-{query_type}-{cache_key_year}'
            result = current_app.cache.get(cache_key)
            if (result is None):
                result = self.fetch_data(query_type, query_year)
                current_app.cache.set(cache_key, result,cache_timeout(cache_invalidate_hour()))

        return result

    def fetch_data(self, query_type, query_year):
        query = self.get_query(query_type, query_year)

        raw = fetch_dataframe(query,query_year)
        results = self.format_results(query_type, raw)
        output = self.get_json(query_type,query_year,results)

        return output

    def get_query(self, query_type, query_year):
        def default():
            return f"SELECT 'query not defined' AS error, '{query_type}' AS query, {query_year} AS year;"

        def standings():
            sql_query = ''

            table_select =  (f'select 	year,'
                                        f'league_standings.league,'
                                        f'league_standings.division,'
                                        f'teams.team_name,'
                                        f'teams.abbreviation,'
                                        f'win,'
                                        f'loss,'
                                        f'win_p,'
                                        f'division_rank,'
                                        f'league_rank,'
                                        f'games_back,'
                                        f'wild_card_back,'
                                        f'elimination_number,'
                                        f'division_elimination_number,'
                                        f'last_10_won,'
                                        f'last_10_lost,'
                                        f'streak,'
                                        f'home_win,'
                                        f'home_loss,'
                                        f'away_win,'
                                        f'away_loss,'
                                        f'al_win,'
                                        f'al_loss,'
                                        f'nl_win,'
                                        f'nl_loss,'
                                        f'c_win,'
                                        f'c_loss,'
                                        f'w_win,'
                                        f'w_loss,'
                                        f'e_win,'
                                        f'e_loss '
                                f'from league_standings '
                                f'inner join teams on teams.team_id = league_standings.team_id '
                                f'where season_type = \'REG\' \n')
            year_select = ''

            if query_year != 'NA':
                year_select = 'and year = %s '
            order_query = 'order by year desc, league, division'
            sql_query = table_select + year_select + order_query

            return sql_query

        queries = {
            "league": standings,
            "division": standings
        }

        return queries.get(query_type, default)()

    def format_results(self, query_type, data):

        def default():
            return data
        def league():
            return data
        def division():
            return data

        formatting = {
            "league": league,
            "division": division
        }

        return formatting.get(query_type, default)()
    
    def get_json(self, query_type, query_year, results):
        
        def default():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)
            
            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(results.to_json(orient='records', date_format='iso'))

        def league():
            records = {}

            yearGroupings = results.groupby('year', sort=False)
            for year, yearValues in yearGroupings:
                records[str(year)] = {}
                leagueGroupings = yearValues.groupby('league')
                for league, leagueValues in leagueGroupings:
                    records[str(year)][league] = json.loads(leagueValues.sort_values(by='league_rank').to_json(orient='records'))

            return json.loads(json.dumps(records))

        def division():
            records = {}

            yearGroupings = results.groupby('year', sort=False)
            for year, yearValues in yearGroupings:
                records[str(year)] = {}
                leagueGroupings = yearValues.groupby('league')
                for league, leagueValues in leagueGroupings:
                    records[str(year)][league] = {}
                    divisionGroupings = leagueValues.groupby(by='division')
                    for division, divisionValues in divisionGroupings:
                        records[str(year)][league][division] = json.loads(divisionValues.sort_values(by='division_rank').to_json(orient='records'))

            return json.loads(json.dumps(records))
        json_data = {
            "league": league,
            "division": division
        }

        return json_data.get(query_type, default)()
