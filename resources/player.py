from flask import current_app
from flask_restful import Resource
from helpers import fetch_dataframe, get_connection, create_player_query, create_player_positions_query, var_dump
from cache import cache_timeout, cache_invalidate_hour
import json as json
import pandas as pd

##
# This is the flask_restful Resource Class for the player API.
# Current Enpoint Structure:
# `/player/${query_type}/${player_id}`
# @param ${query_type}: ('bio'|'stats'|'gamelogs'|'positions'|'repertoire'|'abilities'|'locations'|'locationlogs'|'career'|'')
# @param ${player_id}: ([0-9]*|'All')
##
class Player(Resource):
    def __init__(self):
        self.player_id = 'NA'
        self.first_name = ''
        self.last_name = ''
        self.dob = ''
        self.is_pitcher = False
        self.is_hitter = False
        self.is_active = False
        self.career_stats = {}

    def get(self, query_type='NA', player_id='NA'):
        # We can have an empty query_type or player_id which return the collections of stats.
        if (query_type == 'NA' and (player_id == 'NA' or type(player_id) is int)):
            query_type = 'bio'
        elif (player_id == 'NA' and query_type.isnumeric()):
            player_id = int(query_type)
            query_type = 'bio'

        # Grab basic player data which tells us if we have a pitcher or hitter.
        # Also grabs career & current season stats
        if (type(player_id) is int):
            player_info = self.fetch_result('info', player_id)
            self.player_id = int(player_id)
            self.first_name = player_info[0]['name_first']
            self.last_name = player_info[0]['name_last']
            self.dob = player_info[0]['birth_date']
            self.is_pitcher = bool(player_info[0]['is_pitcher'])
            self.is_active = bool(player_info[0]['is_active'])
            self.career_stats = self.fetch_result('career', player_id)
        
        return self.fetch_result(query_type, player_id)

    
    def fetch_result(self, query_type, player_id):
        # Caching wrapper for fetch_data
        result = None

        if (current_app.config.get('BYPASS_CACHE')):
            print('Bypassing Caching of JSON Results')
            result = self.fetch_data(query_type, player_id)
        else:
            print('Using Cache for JSON Results')
            cache_key_player_id = player_id
            cache_key_resource_type = self.__class__.__name__
            if (player_id == 'NA'):
                cache_key_player_id = 'all'

            cache_key = f'{cache_key_resource_type}-{query_type}-{cache_key_player_id}'
            result = current_app.cache.get(cache_key)
            if (result is None):
                result = self.fetch_data(query_type, player_id)
                current_app.cache.set(cache_key, result,cache_timeout(cache_invalidate_hour()))

        return result

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

        def abilities():
            if (self.is_pitcher):
                return (
                    f'SELECT pitchermlbamid,'
                        f'year,'
                        f'g,'
                        f'ip,'
                        f'batting_average_percentile,'
                        f'hr_9_percentile,'
                        f'era_percentile,'
                        f'k_pct_percentile,'
                        f'bb_pct_percentile,'
                        f'whip_pct_percentile,'
                        f'csw_pct_percentile,'
                        f'o_swing_pct_percentile,'
                        f'babip_pct_percentile,'
                        f'hr_fb_rate_percentile,'
                        f'lob_pct_percentile,'
                        f'flyball_pct_percentile,'
                        f'groundball_pct_percentile,'
                        f'woba_rhb_percentile,'
                        f'woba_lhb_percentile,'
                        f'swinging_strike_pct_percentile,'
                        f'called_strike_pct_percentile,'
                        f'hbp_percentile,'
                        f'batting_average_risp_percentile,'
                        f'batting_average_no_runners,'
                        f'ips_percentile,'
                        f'true_f_strike_pct_percentile '
                    f'FROM mv_pitcher_percentiles '
                    f'WHERE pitchermlbamid = %s;'
                )
            else:
                return (
                    f'SELECT hittermlbamid,'
                        f'year, '
                        f'pa, '
                        f'batting_average_percentile, '
                        f'hr_pa_rate_percentile, '
                        f'r_percentile, '
                        f'rbi_percentile, '
                        f'k_pct_percentile, '
                        f'bb_pct_percentile, '
                        f'sb_percentile, '
                        f'cs_percentile, '
                        f'o_swing_pct_percentile, '
                        f'babip_pct_percentile, '
                        f'flyball_pct_percentile, '
                        f'linedrive_pct_percentile, '
                        f'groundball_pct_percentile, '
                        f'woba_rhb_percentile, '
                        f'woba_lhb_percentile, '
                        f'swinging_strike_pct_percentile, '
                        f'hbp_percentile, '
                        f'triples_percentile, '
                        f'doubles_percentile, '
                        f'ops_percentile, '
                        f'pull_pct_percentile, '
                        f'oppo_pct_percentile, '
                        f'swing_pct_percentile, '
                        f'obp_pct_percentile '
                    f'FROM mv_hitter_percentiles '
                    f'WHERE hittermlbamid = %s;'
                )
        
        def bio():
            return create_player_query(player_id)

        def career():
            if (self.is_pitcher):
                return (
                    f'SELECT year::text AS "year", '
                        f'g, '
                        f'gs, '
                        f'w, '
                        f'l, '
                        f'sv, '
                        f'hld, '
                        f'ip, '
                        f'cg, '
                        f'sho, '
                        f'runs, '
                        f'unearned_runs, '
                        f'earned_runs, '
                        f'era, '
                        f'whip, '
                        f'lob_pct, '
                        f'k_pct, '
                        f'bb_pct, '
                        f'hr_flyball_pct, '
                        f'hbp, '
                        f'wp, '
                        f'teams '
                    f'FROM mv_pitcher_career_stats '
                    f'WHERE pitchermlbamid = %s '
                    f'ORDER BY year ASC;'
                )
            else:
                return (
                    f'SELECT year::text AS "year", '
                        f'g,'
                        f'runs, '
                        f'rbi, '
                        f'sb, '
                        f'cs, '
                        f'teams '
                    f'FROM mv_hitter_career_stats '
                    f'WHERE hittermlbamid = %s '
                    f'ORDER BY year ASC;'
                )

        def gamelogs():
            if (self.is_pitcher):
                return (
                    f'SELECT ghuid AS "gameid",'
                        f'game_played AS "game-date", '
                        f'pitchtype, '
                        f'year_played AS "year",' 
                        f'opponent_handedness AS "split-RL",'
                        f'avg_velocity AS "velo_avg",'
                        f'strikeout_pct,'
                        f'bb_pct,'
                        f'usage_pct,'
                        f'batting_average AS "batting_avg",' 
                        f'o_swing_pct,'
                        f'zone_pct,'
                        f'swinging_strike_pct,'
                        f'called_strike_pct,'
                        f'csw_pct,'
                        f'cswf_pct,'
                        f'plus_pct,'
                        f'foul_pct,'
                        f'contact_pct,'
                        f'o_contact_pct,'
                        f'z_contact_pct,'
                        f'swing_pct,'
                        f'strike_pct,'
                        f'early_called_strike_pct,'
                        f'late_o_swing_pct,'
                        f'f_strike_pct,'
                        f'true_f_strike_pct,'
                        f'groundball_pct,'
                        f'linedrive_pct,'
                        f'flyball_pct,'
                        f'hr_flyball_pct,'
                        f'groundball_flyball_pct,'
                        f'infield_flyball_pct,'
                        f'weak_pct,'
                        f'medium_pct,'
                        f'hard_pct,'
                        f'center_pct,'
                        f'pull_pct,'
                        f'opposite_field_pct,'
                        f'babip_pct,'
                        f'bacon_pct,'
                        f'armside_pct,'
                        f'gloveside_pct,'
                        f'vertical_middle_location_pct AS "v_mid_pct",'
                        f'horizonal_middle_location_pct AS "h_mid_pct",'
                        f'high_pct,'
                        f'low_pct,'
                        f'heart_pct,'
                        f'early_pct,'
                        f'behind_pct,'
                        f'late_pct,'
                        f'non_bip_strike_pct,'
                        f'early_bip_pct,'
                        f'num_pitches AS "pitch-count", num_hit AS "hits", num_bb AS "bb", num_1b AS "1b", num_2b AS "2b", num_3b AS "3b", num_hr AS "hr", num_k AS "k",num_pa AS "pa",num_strikes AS "strikes", num_balls AS "balls", num_foul AS "foul", num_ibb AS "ibb", num_hbp AS "hbp", num_wp AS "wp" '                    
                        f'FROM mv_pitcher_game_logs_2 '
                    f'WHERE pitchermlbamid=%s ' 
                    f'ORDER BY year_played DESC, month_played DESC, ghuid DESC;'
                )
            else:
                return (
                    f'SELECT ghuid AS "gameid",'
                        f'park,'
                        f'team_id AS "team-id",'
                        f'team AS "team",'
                        f'opponent_team_id AS "opponent-team-id",'
                        f'opponent,'
                        f'game_played AS "game-date",'
                        f'team_result AS "team-result",'
                        f'runs_scored AS "runs-scored",'
                        f'opponent_runs_scored AS "opponent-runs-scored",'
                        f'num_plate_appearance AS "pa",' 
                        f'num_hit AS "hits",' 
                        f'num_runs AS "r", '
                        f'num_rbi AS "rbi", '
                        f'num_sb as "sb", '
                        f'num_cs AS "cs", '
                        f'num_bb AS "bb", '
                        f'num_at_bat AS "ab", '
                        f'num_intentional_walk AS "ibb", '
                        f'num_hbp AS "hbp", '
                        f'num_sacrifice AS "sac", ' 
                        f'num_single AS "1b", '
                        f'num_double AS "2b", '
                        f'num_triple AS "3b", '
                        f'num_hr AS "hr", '
                        f'num_total_bases AS "total-bases", '
                        f'num_k AS "k", '
                        f'num_flyball AS "flyball", '
                        f'num_whiff AS "whiff", '
                        f'num_pitches AS "pitch-count", '
                        f'num_barrel AS "barrel", '
                        f'num_called_strike_plus_whiff AS "csw", '
                        f'batting_average AS "batting_avg", '
                        f'onbase_pct, '
                        f'slugging_pct, '
                        f'strikeout_pct, '
                        f'bb_pct, '
                        f'babip_pct, '
                        f'hr_fb_pct, '
                        f'swinging_strike_pct, '
                        f'csw_pct, '
                        f'barrel_pct '
                    f'FROM mv_hitter_game_logs '
                    f'WHERE hittermlbamid=%s ' 
                    f'ORDER BY year_played DESC, month_played DESC, ghuid DESC;'
                )

        def info():
            return (
                f'SELECT name_first,'
                    f'name_last,'
                    f'birth_date,'
                    f'ispitcher AS "is_pitcher",'
                    f'isactive AS "is_active" '
                f'FROM pl_players '
                f'WHERE mlbamid=%s;'
            )

        def locationlogs():
            if (self.is_pitcher):
                return (
                    f'SELECT DISTINCT ghuid AS "gameid",'
                        f'pitchtype,'
                        f'hitterside AS "split-RL",'
                        f'pitch_locations, '
                        f'num_pitches AS "pitch-count", '
                        f'usage_pct, '
                        f'whiff, '
                        f'called_strike, '
                        f'csw_pct, '
                        f'zone_pct, '
                        f'zone_swing_pct, '
                        f'swinging_strike_pct, '
                        f'o_swing_pct, '
                        f'avg_velocity '
                    f'FROM mv_pitcher_game_log_pitches '
                    f"WHERE pitchermlbamid = %s "
                    f'ORDER BY ghuid;'
                )
            else:
                return (
                    f'SELECT ghuid AS "gameid",'
                        f'pitchtype,'
                        f'hitterside AS "split-RL",'
                        f'array_agg(pitch_location) AS "pitch_locations" '
                    f'FROM pl_leaderboard_v2 '
                    f"WHERE pitchermlbamid = %s "
                    f'GROUP BY ghuid, pitchtype, hitterside '
                    f'ORDER BY ghuid;'
                )

        def locations():
            if (self.is_pitcher):
                return(
                    f'SELECT pitchtype,' 
                        f'year_played AS "year",' 
                        f'opponent_handedness AS "split-RL",'
                        f'home_away AS "split-HA",'
                        f'pitch_locations '
                    f'FROM player_page_repertoire '
                    f"WHERE pitchermlbamid = %s "
                    f"AND pitchtype <> 'All' AND year_played <> 'All' "
                    f'ORDER BY pitchtype, year_played, opponent_handedness, home_away;'
                )
            else:
                return(
                    f'SELECT pitchtype,' 
                        f'year_played AS "year",' 
                        f'opponent_handedness AS "split-RL",'
                        f'home_away AS "split-HA",'
                        f'pitch_locations '
                    f'FROM player_page_repertoire '
                    f"WHERE pitchermlbamid = %s "
                    f"AND pitchtype <> 'All' AND year_played <> 'All' "
                    f'ORDER BY pitchtype, year_played, opponent_handedness, home_away;'
                )    

        def positions():
            # TODOD: Add in filtering by hitter/pitcher as playerid (complementing 'all' player_id)
            return create_player_positions_query(player_id)

        def stats():
            if (self.is_pitcher):
                return (
                    f'SELECT pitchtype,' 
                        f'year_played AS "year",' 
                        f'opponent_handedness AS "split-RL",'
                        f'home_away AS "split-HA",'
                        f'avg_velocity AS "velo_avg",'
                        f'k_pct,'
                        f'bb_pct,'
                        f'usage_pct,'
                        f'batting_average AS "batting_avg",' 
                        f'o_swing_pct,'
                        f'zone_pct,'
                        f'swinging_strike_pct,'
                        f'called_strike_pct,'
                        f'csw_pct,'
                        f'cswf_pct,'
                        f'plus_pct,'
                        f'foul_pct,'
                        f'contact_pct,'
                        f'o_contact_pct,'
                        f'z_contact_pct,'
                        f'swing_pct,'
                        f'strike_pct,'
                        f'early_called_strike_pct,'
                        f'late_o_swing_pct,'
                        f'f_strike_pct,'
                        f'true_f_strike_pct,'
                        f'groundball_pct,'
                        f'linedrive_pct,'
                        f'flyball_pct,'
                        f'hr_flyball_pct,'
                        f'groundball_flyball_pct,'
                        f'infield_flyball_pct,'
                        f'weak_pct,'
                        f'medium_pct,'
                        f'hard_pct,'
                        f'center_pct,'
                        f'pull_pct,'
                        f'opposite_field_pct,'
                        f'babip_pct,'
                        f'bacon_pct,'
                        f'armside_pct,'
                        f'gloveside_pct,'
                        f'vertical_middle_location_pct AS "v_mid_pct",'
                        f'horizonal_middle_location_pct AS "h_mid_pct",'
                        f'high_pct,'
                        f'low_pct,'
                        f'heart_pct,'
                        f'early_pct,'
                        f'behind_pct,'
                        f'late_pct,'
                        f'non_bip_strike_pct,'
                        f'early_bip_pct,'
                        f'num_pitches AS "pitch-count", num_hits AS "hits", num_bb AS "bb", num_1b AS "1b", num_2b AS "2b", num_3b AS "3b", num_hr AS "hr", num_k AS "k",num_pa AS "pa",num_strike AS "strikes", num_ball AS "balls", num_foul AS "foul", num_ibb AS "ibb", num_hbp AS "hbp", num_wp AS "wp" '
                    f'FROM player_page_repertoire '
                    f"WHERE pitchermlbamid = %s "
                    f'ORDER BY pitchtype, year_played, opponent_handedness, home_away;'
                )
            else:
                return (
                    f'SELECT ' 
                        f'year_played AS "year",' 
                        f'opponent_handedness AS "split-RL",'
                        f'home_away AS "split-HA",'
                        f'batting_average AS "batting_avg",' 
                        f'o_swing_pct,'
                        f'zone_pct,'
                        f'swinging_strike_pct,'
                        f'called_strike_pct,'
                        f'avg_velocity AS "velo_avg",'
                        f'foul_pct,'
                        f'plus_pct,'
                        f'contact_pct,'
                        f'o_contact_pct,'
                        f'z_contact_pct,'
                        f'swing_pct,'
                        f'strike_pct,'
                        f'early_called_strike_pct,'
                        f'late_o_swing_pct,'
                        f'f_strike_pct,'
                        f'true_f_strike_pct,'
                        f'groundball_pct,'
                        f'linedrive_pct,'
                        f'flyball_pct,'
                        f'infield_flyball_pct,'
                        f'weak_pct,'
                        f'medium_pct,'
                        f'hard_pct,'
                        f'pull_pct,'
                        f'opposite_field_pct,'
                        f'babip_pct,'
                        f'bacon_pct,'
                        f'armside_pct,'
                        f'gloveside_pct,'
                        f'vertical_middle_location_pct AS "v_mid_pct",'
                        f'horizonal_middle_location_pct AS "h_mid_pct",'
                        f'high_pct,'
                        f'low_pct,'
                        f'heart_pct,'
                        f'early_pct,'
                        f'behind_pct,'
                        f'late_pct,'
                        f'non_bip_strike_pct,'
                        f'early_bip_pct,'
                        f'onbase_pct,'
                        f'k_pct,'
                        f'bb_pct,'
                        f'early_o_contact_pct,'
                        f'late_o_contact_pct,'
                        f'first_pitch_swing_pct,'
                        f'num_pitches AS "pitch-count", num_hits AS "hits", num_rbi AS "rbi", num_bb AS "bb", num_1b AS "1b", num_2b AS "2b", num_3b AS "3b", num_hr AS "hr", num_k AS "k",num_pa AS "pa",num_strike AS "strikes", num_ball AS "balls" '
                    f'FROM mv_hitter_page_stats '
                    f"WHERE hittermlbamid = %s "
                    f'ORDER BY year_played, opponent_handedness, home_away;'
                )

        queries = {
            "abilities": abilities,
            "bio": bio,
            "career": career,
            "gamelogs": gamelogs,
            "info": info,
            "locationlogs": locationlogs,
            "locations": locations,
            "positions": positions,
            "repertoire": stats,
            "stats": stats
        }

        return queries.get(query_type, default)()

    def format_results(self, query_type, data):

        def default():
            return data

        def career():
            if (self.is_pitcher):
                data['ip'] = pd.to_numeric(data['ip'], downcast='integer')
                data[['g','gs','w','l','sv','hld','cg','sho']] = data[['g','gs','w','l','sv','hld','cg','sho']].apply(pd.to_numeric,downcast='integer')

            formatted_data = data.set_index(['year'])
            return formatted_data

        def locationlogs():
            formatted_results = data.set_index(['gameid','pitchtype','split-RL'])
            
            return formatted_results
        
        def gamelogs():
            #if (self.is_pitcher):
            #data[['win','loss','save','hold','ip','hits','r','er','bb','k','pitch-count','pa','ab','hbp','hr','flyball','sac','whiff','csw','strikeout_pct','bb_pct','babip_pct','hr_fb_pct','left_on_base_pct','swinging_strike_pct','csw_pct']] = data[['win','loss','save','hold','ip','hits','r','er','bb','k','pitch-count','pa','ab','hbp','hr','flyball','sac','whiff','csw','strikeout_pct','bb_pct','babip_pct','hr_fb_pct','left_on_base_pct','swinging_strike_pct','csw_pct']].apply(pd.to_numeric,downcast='integer')
            #else: 
            #    data[['runs-scored','opponent-runs-scored','save','hold','ip','hits','r','er','bb','k','pitch-count','pa','ab','hbp','hr','flyball','sac','whiff','csw','strikeout_pct','bb_pct','babip_pct','hr_fb_pct','left_on_base_pct','swinging_strike_pct','csw_pct']] = data[['win','loss','save','hold','ip','hits','r','er','bb','k','pitch-count','pa','ab','hbp','hr','flyball','sac','whiff','csw','strikeout_pct','bb_pct','babip_pct','hr_fb_pct','left_on_base_pct','swinging_strike_pct','csw_pct']].apply(pd.to_numeric,downcast='integer')

            formatted_data = data.set_index(['gameid','pitchtype','split-RL'])
            return formatted_data

        def stats():
            if (self.is_pitcher):
                formatted_results = data.set_index(['pitchtype','year','split-RL','split-HA'])
            else:
                formatted_results = data.set_index(['year','split-RL','split-HA'])

            return formatted_results

        formatting = {
            "career": career,
            "gamelogs": gamelogs,
            "locationlogs": locationlogs,
            "locations": stats,
            "repertoire": stats,
            "stats": stats
        }

        return formatting.get(query_type, default)()
    
    def get_json(self, query_type, player_id, results):
        
        def default():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)
            
            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(results.to_json(orient='records', date_format='iso'))

        def bio():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)
            results['lastgame'] = pd.to_datetime(results['lastgame']).dt.strftime("%a %m/%d/%Y")
            results['birth_date'] = pd.to_datetime(results['birth_date']).dt.strftime("%a %m/%d/%Y")

            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(results.to_json(orient='records', date_format='iso'))
        
        def career():
            results.fillna(value=0, inplace=True)            
            output = json.loads(results.to_json(orient='index'))
            
            return output

        def gamelogs():
            results.fillna(value=0, inplace=True)

            # Functionality Removed.
            # Set up columnar data for local browser storage and filters
            # Front end can quickly slice on lookup of index in game_id_index data hash
            # hits = results['hits'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            
            if (self.is_pitcher):

                # Convert datetime to usable json format
                results['game-date'] = pd.to_datetime(results['game-date']).dt.strftime("%a %m/%d/%Y")

                output_dict = { 'player_id': player_id, 'is_pitcher': self.is_pitcher, 'is_active': self.is_active, 'logs': {} }

                # Drop cols that are not displayed on the front end
                # TODO: Add cols here that can safely be dropped as they are not used on the frontend.
                #results.drop(columns=['start','sac','csw'], inplace=True)
                
                # Ensure we have valid data for NaN entries using json.dumps of Python None object
                result_dict = json.loads(results.to_json(orient='index'))

                var_dump(result_dict)

                for keys, value in result_dict.items():
                    # json coversion returns tuple string
                    key = eval(keys)
                    gameid_key = key[0]
                    if gameid_key not in output_dict['logs']:
                        output_dict['logs'][gameid_key] = {'game':{}, 'pitches':{}}

                    pitch_key = key[1]

                    if pitch_key not in output_dict['logs'][gameid_key]['pitches']:
                        output_dict['logs'][gameid_key]['pitches'][pitch_key] = {'splits':{}}
                    
                    rl_split_key = key[2]
                    if rl_split_key not in output_dict['logs'][gameid_key]['pitches'][pitch_key]['splits']:
                        output_dict['logs'][gameid_key]['pitches'][pitch_key]['splits'][rl_split_key] = value
                
                return output_dict

            else:
                # Convert datetime to usable json format
                results['game-date'] = pd.to_datetime(results['game-date']).dt.strftime("%a %m/%d/%Y")

                output_dict = { 'player_id': player_id, 'is_pitcher': self.is_pitcher, 'is_active': self.is_active, 'logs': {} }
                
                result_dict = json.loads(results.to_json(orient='index'))
                index = 0
                
                for key, value in result_dict.items():
                    output_dict['data']['game_id_index'][key] = index
                    output_dict['logs'][key] = value
                    output_dict['logs'][key]['index'] = index
                    index += 1

                return output_dict
                
        def locationlogs():
            output_dict = { 'player_id': player_id, 'is_pitcher': self.is_pitcher, 'is_active': self.is_active, 'logs': {} }
            results.fillna(value=0, inplace=True)
            result_dict = json.loads(results.to_json(orient='index'))
            
            for keys, value in result_dict.items():
                # json coversion returns tuple string
                key = eval(keys)
                gameid_key = key[0]
                if gameid_key not in output_dict['logs']:
                    output_dict['logs'][gameid_key] = {'pitches':{}}

                pitch_key = key[1]

                if pitch_key not in output_dict['logs'][gameid_key]['pitches']:
                    output_dict['logs'][gameid_key]['pitches'][pitch_key] = {'splits':{}}
                
                rl_split_key = key[2]
                if rl_split_key not in output_dict['logs'][gameid_key]['pitches'][pitch_key]['splits']:
                    output_dict['logs'][gameid_key]['pitches'][pitch_key]['splits'][rl_split_key] = value
            
            return output_dict

        def stats():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)

            result_dict = json.loads(results.to_json(orient='index'))

            if (self.is_pitcher):
                # Sort our DataFrame so we have a prettier JSON format for the API
                output_dict = { 'player_id': player_id, 'is_pitcher': self.is_pitcher, 'is_active': self.is_active, query_type: {'pitches':{}} }

                # Make sure our index keys exist in our dict structure then push on our data values
                for keys, value in result_dict.items():
                    # json coversion returns tuple string
                    key = eval(keys)
                    pitch_key = key[0]

                    if pitch_key not in output_dict[query_type]['pitches']:
                        output_dict[query_type]['pitches'][pitch_key] = {'years':{}}

                    year_key = key[1]
                    stats = { 'total': self.career_stats[year_key], 'splits':{} } if (pitch_key == 'All') else { 'splits':{} }
                    if year_key not in output_dict[query_type]['pitches'][pitch_key]['years']:
                        output_dict[query_type]['pitches'][pitch_key]['years'][year_key] = stats
                    
                    rl_split_key = key[2]
                    if rl_split_key not in output_dict[query_type]['pitches'][pitch_key]['years'][year_key]['splits']:
                        output_dict[query_type]['pitches'][pitch_key]['years'][year_key]['splits'][rl_split_key] = {'park':{}}
                
                    ha_split_key = key[3]
                    output_dict[query_type]['pitches'][pitch_key]['years'][year_key]['splits'][rl_split_key]['park'][ha_split_key] = value
            else:
                # Sort our DataFrame so we have a prettier JSON format for the API
                output_dict = { 'player_id': player_id, 'is_pitcher': self.is_pitcher, 'is_active': self.is_active, query_type: {'years':{}} }

                # Make sure our index keys exist in our dict structure then push on our data values
                for keys, value in result_dict.items():
                    # json coversion returns tuple string
                    key = eval(keys)

                    year_key = key[0]
                    if year_key not in output_dict[query_type]['years']:
                        output_dict[query_type]['years'][year_key] = { 'splits':{} }
                    
                    rl_split_key = key[1]
                    if rl_split_key not in output_dict[query_type]['years'][year_key]['splits']:
                        output_dict[query_type]['years'][year_key]['splits'][rl_split_key] = {'park':{}}
                
                    ha_split_key = key[2]
                    output_dict[query_type]['years'][year_key]['splits'][rl_split_key]['park'][ha_split_key] = value
            
            return output_dict

        json_data = {
            "bio": bio,
            "career": career,
            "gamelogs": gamelogs,
            "locationlogs": locationlogs,
            "locations": stats, 
            "repertoire": stats,
            "stats": stats
        }

        return json_data.get(query_type, default)()

