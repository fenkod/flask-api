from flask import current_app
from flask_restful import Resource
from helpers import fetch_dataframe, get_connection, create_player_query, create_player_positions_query, var_dump
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

        var_dump(current_app.config.get('BYPASS_CACHE'))
        if (current_app.config.get('BYPASS_CACHE')):
            print('Bypassing Caching of JSON Results')
            result = self.fetch_data(query_type, player_id)
        else:
            print('Using Cache for JSON Results')
            cache_key = f'{cache_key_resource_type}-{query_type}-{cache_key_player_id}'
            result = current_app.cache.get(cache_key)
            if (result is None):
                result = self.fetch_data(query_type, player_id)
                current_app.cache.set(cache_key, result,cache_timeout(cache_invalidate_hour()))

        return (result)

    def fetch_data(self, query_type, player_id):
        query = self.get_query(query_type, player_id)
        query_var=None
        if (type(player_id) is int):
            query_var = player_id

        raw = fetch_dataframe(query,query_var)
        results = self.format_results(query_type, raw)
        output = self.get_json(query_type,player_id,results)

        return output

    def get_query(self, query_type, player_id):
        def default():
            return f"SELECT 'query not defined' AS error, '{query_type}' AS query, {player_id} AS id;"

        def gamelogs():
            return (
                f'SELECT ghuid AS "mlb_game_id",'
                    f'thrown_for_team AS "team_id",'
                    f'team,'
                    f'thrown_against_team AS "opponent_team_id",'
                    f'opponent,'
                    f'game_played AS "game_date",'
                    f'start, win, loss, save, hold,' 
                    f'num_ip AS "ip", num_hit AS "hits", num_runs AS "r", num_earned_runs AS "er", num_bb AS "bb", num_k AS "k", num_pitches AS "pitch_count",' 
                    f'num_pa AS "pa", num_ab AS "ab", num_hbp AS "hbp", num_hr AS "hr", num_flyball AS "fb", num_sacrifice AS "sac", num_whiff as "whiff", num_called_strike_plus_whiff AS "csw",'
                    f'strikeout_pct, bb_pct, babip_pct, hr_fb_pct, left_on_base_pct, swinging_strike_pct, csw_pct '
                f'FROM mv_pitcher_game_logs '
                f'WHERE pitchermlbamid={player_id} ' 
                f'ORDER BY year_played DESC, month_played DESC, ghuid DESC;'
            )

        def positions():
            return create_player_positions_query(player_id)

        def repertoire():
            return (
                f'SELECT pitchtype AS "pitch",' 
                    f'year_played AS "year",' 
                    f'opponent_handedness AS "split-RL",'
                    f'home_away AS "split-HA",'
                    f'usage_pct AS "usage",'
                    f'batting_average AS "avg",' 
                    f'o_swing_pct AS "o-swing",'
                    f'zone_pct AS "zone",'
                    f'swinging_strike_pct AS "swinging-strike",'
                    f'called_strike_pct AS "called-strike",'
                    f'csw_pct AS "csw" from player_page_repertoire '
                f"WHERE pitchermlbamid = {player_id} "
                f'ORDER BY pitchtype, year_played, opponent_handedness, home_away;'
            )
        
        def stats():
            return create_player_query(player_id)
        
        queries = {
            "gamelogs": gamelogs,
            "positions": positions,
            "repertoire": repertoire,
            "stats": stats
        }

        return queries.get(query_type, default)()

    def format_results(self, query_type, data):

        def default():
            return data

        def gamelogs():
            data[['win','loss','save','hold','ip','hits','r','er','bb','k','pitch_count','pa','ab','hbp','hr','fb','sac','whiff','csw','strikeout_pct','bb_pct','babip_pct','hr_fb_pct','left_on_base_pct','swinging_strike_pct','csw_pct']] = data[['win','loss','save','hold','ip','hits','r','er','bb','k','pitch_count','pa','ab','hbp','hr','fb','sac','whiff','csw','strikeout_pct','bb_pct','babip_pct','hr_fb_pct','left_on_base_pct','swinging_strike_pct','csw_pct']].apply(pd.to_numeric)
            formatted_data = data.set_index(['mlb_game_id'])
            return formatted_data

        def repertoire():
            data['year'] = pd.to_numeric(data['year'], downcast='integer')
            
            data[['usage','avg','o-swing','zone','swinging-strike','called-strike','csw']] = data[['usage','avg','o-swing','zone','swinging-strike','called-strike','csw']].apply(pd.to_numeric)
            formatted_data = data.set_index(['pitch','year','split-RL','split-HA'])
            return formatted_data

        formatting = {
           "repertoire": repertoire,
           "gamelogs": gamelogs
        }

        return formatting.get(query_type, default)()
    
    def get_json(self, query_type, player_id, results):
        
        def default():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)
            
            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(results.to_json(orient='records', date_format='iso'))

        def gamelogs():
            
            # Set up columnar data for local browser storage and filters
            # Front end can quickly slice on lookup of index in game_id_index data hash
            start = results['start'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            win = results['win'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            loss = results['loss'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            save = results['save'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            hold = results['hold'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            ip = results['ip'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            hits = results['hits'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            r = results['r'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            er = results['er'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            bb = results['bb'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            k = results['k'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            pitch_count = results['pitch_count'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            pa = results['pa'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            ab = results['ab'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            hbp = results['hbp'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            hr = results['hr'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            fb = results['fb'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            sac = results['sac'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            whiff = results['whiff'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            csw = results['csw'].to_numpy(dtype=int,copy=True,na_value=0).tolist()

            # Convert datetime to usable json format
            results['game_date'] = pd.to_datetime(results['game_date']).dt.strftime("%a %m/%d/%Y")


            output_dict = { 'player_id': player_id, 'data': { 'game_id_index':{}, 'start': start, 'win': win, 'loss': loss, 'save': save, 'hold': hold, 'ip': ip, 'hits': hits, 'r': r, 'er': er, 'bb': bb, 'k': k, 'pitch_count': pitch_count, 'pa': pa, 'ab': ab, 'hbp': hbp, 'hr': hr, 'fb': fb, 'sac': sac, 'whiff': whiff, 'csw': csw }, 'logs': {} }

            results.drop(columns=['start','pa','ab','hbp','hr','fb','sac','whiff','csw'], inplace=True)
            
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)
            result_dict = results.to_dict(orient='index')
            index = 0
            
            for key, value in result_dict.items():
                output_dict['data']['game_id_index'][key] = index
                output_dict['logs'][key] = value
                output_dict['logs'][key]['index'] = index
                index += 1

            return output_dict

        def repertoire():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)

            # Sort our DataFrame so we have a prettier JSON format for the API
            output_dict = { 'player_id': player_id, query_type: {'pitches':{}} }

            result_dict = results.to_dict(orient='index')
            # Make sure our index keys exist in our dict structure then push on our data values
            for key, value in result_dict.items():
                
                pitch_key = key[0]
                if pitch_key not in output_dict[query_type]['pitches']:
                    output_dict[query_type]['pitches'][pitch_key] = {'years':{}}

                year_key = key[1]
                if year_key not in output_dict[query_type]['pitches'][pitch_key]['years']:
                    output_dict[query_type]['pitches'][pitch_key]['years'][year_key] = {'splits':{}}
                
                rl_split_key = key[2]
                if rl_split_key not in output_dict[query_type]['pitches'][pitch_key]['years'][year_key]['splits']:
                    output_dict[query_type]['pitches'][pitch_key]['years'][year_key]['splits'][rl_split_key] = {'park':{}}
            
                ha_split_key = key[3]
                output_dict[query_type]['pitches'][pitch_key]['years'][year_key]['splits'][rl_split_key]['park'][ha_split_key] = value
            return output_dict

        json_data = {
            "repertoire": repertoire,
            "gamelogs": gamelogs 
        }

        return json_data.get(query_type, default)()

