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
                        f'park,'
                        f'thrown_for_team AS "team-id",'
                        f'team,'
                        f'thrown_against_team AS "opponent-team-id",'
                        f'opponent,'
                        f'game_played AS "game-date",'
                        f'team_result AS "team-result",'
                        f'runs_scored AS "runs-scored",'
                        f'opponent_runs_scored AS "opponent-runs-scored",'
                        f'start, win, loss, save, hold,' 
                        f'num_ip AS "ip",'
                        f'num_hit AS "hits",'
                        f'num_runs AS "r",' 
                        f'num_earned_runs AS "er",'
                        f'num_bb AS "bb",'
                        f'num_k AS "k",' 
                        f'num_pitches AS "pitch-count",' 
                        f'num_pa AS "pa",'
                        f'num_ab AS "ab",' 
                        f'num_hbp AS "hbp",' 
                        f'num_hr AS "hr",'
                        f'num_flyball AS "flyball",'
                        f'num_sacrifice AS "sac",'
                        f'num_whiff as "whiff",' 
                        f'num_called_strike_plus_whiff AS "csw",'
                        f'num_strikes AS "strikes",'
                        f'num_balls AS "balls",'
                        f'num_zone AS "zone",'
                        f'total_velo AS "total-velo",'
                        f'num_velo AS "pitches-clocked",'
                        f'num_armside AS "armside",'
                        f'num_gloveside AS "gloveside",'
                        f'num_inside AS "inside",'
                        f'num_outside AS "outside",'
                        f'num_horizontal_middle AS "h-mid",'
                        f'num_high AS "high",'
                        f'num_middle AS "mid",'
                        f'num_low AS "low",'
                        f'num_heart AS "heart",'
                        f'num_early AS "early",'
                        f'num_late AS "late",'
                        f'num_behind AS "behind",'
                        f'num_non_bip_strike AS "non-bip-strike",'
                        f'num_batted_ball_event AS "batted-ball-event",'
                        f'num_early_bip AS "early-bip",'
                        f'num_fastball AS "fastball",'
                        f'num_secondary AS "secondary",'
                        f'num_early_secondary AS "early-secondary",'
                        f'num_late_secondary AS "late-secondary",'
                        f'num_called_strike AS "called-strike",'
                        f'num_early_called_strike AS "early-called-strike",'
                        f'num_put_away AS "putaway",'
                        f'num_swing AS "swings",'
                        f'num_contact AS "contact",'
                        f'num_foul AS "foul",'
                        f'num_first_pitch_swing AS "first-pitch-swings",'
                        f'num_first_pitch_strike AS "first-pitch-strikes",'
                        f'num_true_first_pitch_strike AS "true-first-pitch-strikes",'
                        f'num_plus_pitch AS "plus-pitch",'
                        f'num_z_swing AS "z-swing",'
                        f'num_z_contact AS "z-contact",'
                        f'num_o_swing AS "o-swing",'
                        f'num_o_contact AS "o-contact",'
                        f'num_early_o_swing AS "early-o-swing",'
                        f'num_early_o_contact AS "early-o-contact",'
                        f'num_late_o_swing AS "late-o-swing",'
                        f'num_late_o_contact AS "late-o-contact",'
                        f'num_pulled_bip AS "pulled-bip",'
                        f'num_opposite_bip AS "opp-bip",'
                        f'num_line_drive AS "line-drive",'
                        f'num_if_fly_ball AS "if-flyball",'
                        f'num_ground_ball AS "groundball",'
                        f'num_weak_bip AS "weak-bip",'
                        f'num_medium_bip AS "medium-bip",'
                        f'num_hard_bip AS "hard-bip",'
                        f'num_1b AS "1b",'
                        f'num_2b AS "2b",'
                        f'num_3b AS "3b",'
                        f'num_ibb AS "ibb",'
                        f'strikeout_pct,'
                        f'bb_pct,'
                        f'babip_pct,'
                        f'hr_fb_pct,'
                        f'left_on_base_pct,'
                        f'swinging_strike_pct,'
                        f'csw_pct,'
                        f'avg_velocity AS "velo_avg",'
                        f'foul_pct,'
                        f'plus_pct,'
                        f'first_pitch_swing_pct,'
                        f'early_o_contact_pct,'
                        f'late_o_contact_pct,'
                        f'armside_pct,'
                        f'gloveside_pct,'
                        f'inside_pct,'
                        f'outside_pct,'
                        f'high_pct,'
                        f'horizonal_middle_location_pct AS "h_mid_pct",'
                        f'vertical_middle_location_pct AS "v_mid_pct",'
                        f'low_pct,'
                        f'heart_pct,'
                        f'early_pct,'
                        f'behind_pct,'
                        f'late_pct,'
                        f'zone_pct,'
                        f'non_bip_strike_pct,'
                        f'early_bip_pct,'
                        f'groundball_pct,'
                        f'linedrive_pct,'
                        f'flyball_pct,'
                        f'infield_flyball_pct,'
                        f'weak_pct,'
                        f'medium_pct,'
                        f'hard_pct,'
                        f'pull_pct,'
                        f'opposite_field_pct,'
                        f'swing_pct,'
                        f'o_swing_pct,'
                        f'z_swing_pct,'
                        f'contact_pct,'
                        f'o_contact_pct,'
                        f'z_contact_pct,'
                        f'called_strike_pct,'
                        f'early_called_strike_pct,'
                        f'late_o_swing_pct,'
                        f'f_strike_pct,'
                        f'true_f_strike_pct,'
                        f'put_away_pct,'
                        f'batting_average AS "batting_avg",'
                        f'walk_pct,'
                        f'bacon_pct,'
                        f'on_base_pct,'
                        f'whip AS "whip_pct" '
                    f'FROM mv_pitcher_game_logs '
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
            if (self.is_pitcher):
                data[['win','loss','save','hold','ip','hits','r','er','bb','k','pitch-count','pa','ab','hbp','hr','flyball','sac','whiff','csw','strikeout_pct','bb_pct','babip_pct','hr_fb_pct','left_on_base_pct','swinging_strike_pct','csw_pct']] = data[['win','loss','save','hold','ip','hits','r','er','bb','k','pitch-count','pa','ab','hbp','hr','flyball','sac','whiff','csw','strikeout_pct','bb_pct','babip_pct','hr_fb_pct','left_on_base_pct','swinging_strike_pct','csw_pct']].apply(pd.to_numeric,downcast='integer')
            #else: 
            #    data[['runs-scored','opponent-runs-scored','save','hold','ip','hits','r','er','bb','k','pitch-count','pa','ab','hbp','hr','flyball','sac','whiff','csw','strikeout_pct','bb_pct','babip_pct','hr_fb_pct','left_on_base_pct','swinging_strike_pct','csw_pct']] = data[['win','loss','save','hold','ip','hits','r','er','bb','k','pitch-count','pa','ab','hbp','hr','flyball','sac','whiff','csw','strikeout_pct','bb_pct','babip_pct','hr_fb_pct','left_on_base_pct','swinging_strike_pct','csw_pct']].apply(pd.to_numeric,downcast='integer')

            formatted_data = data.set_index(['gameid'])
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

            # Common Stats
            hits = results['hits'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            r = results['r'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            bb = results['bb'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            k = results['k'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            pitch_count = results['pitch-count'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            pa = results['pa'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            ab = results['ab'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            hbp = results['hbp'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            hr = results['hr'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            fb = results['flyball'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            sac = results['sac'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            whiff = results['whiff'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
            csw = results['csw'].to_numpy(dtype=int,copy=True,na_value=0).tolist()

            if (self.is_pitcher):

                mockjson = """{
    "player_id": 669456,
    "is_pitcher": true,
    "is_active": true,
    "data": {
        "game_id_index": {
            "2020/09/23/chamlb-clemlb-1": 0,
            "2020/09/17/clemlb-detmlb-1": 1
        },
        "start": [
            1,
            1
        ],
        "win": [
            0,
            1
        ],
        "loss": [
            0,
            0
        ],
        "save": [
            0,
            0
        ],
        "hold": [
            0,
            0
        ],
        "ip": [
            5,
            7
        ],
        "hits": [
            2,
            4
        ],
        "r": [
            1,
            3
        ],
        "er": [
            0,
            3
        ],
        "bb": [
            3,
            2
        ],
        "k": [
            10,
            10
        ],
        "pitch_count": [
            98,
            118
        ],
        "pa": [
            23,
            30
        ],
        "ab": [
            20,
            28
        ],
        "hbp": [
            0,
            0
        ],
        "hr": [
            0,
            1
        ],
        "flyball": [
            2,
            8
        ],
        "sac": [
            0,
            0
        ],
        "whiff": [
            16,
            21
        ],
        "csw": [
            31,
            39
        ]
    },
    "logs": {
        "2018/05/31/clemlb-minmlb-1": {
            "index": 0,
            "game": {
                "gs": 1,
                "w": 1,
                "l": 0,
                "sv": 0,
                "hld": 0,
                "ip": 6.2,
                "r": 1,
                "er": 0,
                "lob": 5,
                "lob_pct": 15.0,
                "park": "HOME",
                "team-id": 17,
                "team": "CLE",
                "opponent-team-id": 12,
                "opponent": "CWS",
                "game-date": "Wed 09/23/2020",
                "team-result": "Win",
                "runs-scored": 3.0,
                "opponent-runs-scored": 2.0
            },
            "pitches": {
                "ALL": {
                    "splits": {
                        "ALL": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "L": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "R": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        }
                    }
                },
                "CH": {
                    "splits": {
                        "ALL": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "L": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        }
                    }
                },
                "CU": {
                    "splits": {
                        "ALL": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "L": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "R": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        }
                    }
                },
                "FA": {
                    "splits": {
                        "ALL": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "L": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "R": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        }
                    }
                },
                "SL": {
                    "splits": {
                        "ALL": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "L": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "R": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        }
                    }
                }
            }
        },
        "2018/06/17/minmlb-clemlb-1": {
            "index": 1,
            "game": {
                "gs": 1,
                "w": 1,
                "l": 0,
                "sv": 0,
                "hld": 0,
                "ip": 6.2,
                "r": 1,
                "er": 0,
                "lob": 5,
                "lob_pct": 15.0,
                "park": "HOME",
                "team-id": 17,
                "team": "CLE",
                "opponent-team-id": 12,
                "opponent": "CWS",
                "game-date": "Wed 09/23/2020",
                "team-result": "Win",
                "runs-scored": 3.0,
                "opponent-runs-scored": 2.0
            },
            "pitches": {
                "ALL": {
                    "splits": {
                        "ALL": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "L": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "R": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        }
                    }
                },
                "CH": {
                    "splits": {
                        "ALL": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "L": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        }
                    }
                },
                "CU": {
                    "splits": {
                        "ALL": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "L": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "R": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        }
                    }
                },
                "FA": {
                    "splits": {
                        "ALL": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "L": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "R": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        }
                    }
                },
                "SL": {
                    "splits": {
                        "ALL": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "L": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        },
                        "R": {
                            "hits": 2,
                            "r": 1,
                            "er": 0,
                            "bb": 3,
                            "k": 10,
                            "lob": 5,
                            "lob_pct": 15.0,
                            "pitch-count": 98,
                            "hbp": 0,
                            "hr": 0,
                            "flyball": 2,
                            "whiff": 16,
                            "strikes": 57,
                            "balls": 41,
                            "zone": 37,
                            "total-velo": 8833.0,
                            "pitches-clocked": 98,
                            "armside": 21,
                            "gloveside": 55,
                            "inside": 15,
                            "outside": 61,
                            "h-mid": 22,
                            "high": 30,
                            "mid": 15,
                            "low": 53,
                            "heart": 3,
                            "early": 50,
                            "late": 27,
                            "behind": 35,
                            "non-bip-strike": 49,
                            "batted-ball-event": 8,
                            "early-bip": 2,
                            "fastball": 46,
                            "secondary": 52,
                            "early-secondary": 21,
                            "late-secondary": 14,
                            "called-strike": 15,
                            "early-called-strike": 9,
                            "putaway": 9,
                            "swings": 42,
                            "contact": 26,
                            "foul": 18,
                            "first-pitch-swings": 8,
                            "first-pitch-strikes": 12,
                            "true-first-pitch-strikes": 11,
                            "plus-pitch": 42,
                            "z-swing": 25,
                            "z-contact": 18,
                            "o-swing": 17,
                            "o-contact": 8,
                            "early-o-swing": 7,
                            "early-o-contact": 4,
                            "late-o-swing": 9,
                            "late-o-contact": 4,
                            "pulled-bip": 6,
                            "opp-bip": 1,
                            "line-drive": 1,
                            "if-flyball": 0,
                            "groundball": 6,
                            "weak-bip": 2,
                            "medium-bip": 5,
                            "hard-bip": 1,
                            "1b": 1,
                            "2b": 1,
                            "3b": 0,
                            "ibb": 0,
                            "strikeout_pct": 43.5,
                            "bb_pct": 13.0,
                            "babip_pct": 0.2,
                            "hr_fb_pct": 0.0,
                            "left_on_base_pct": 80.0,
                            "swinging_strike_pct": 16.3,
                            "csw_pct": 31.6,
                            "velo_avg": 90.1,
                            "foul_pct": 18.4,
                            "plus_pct": 42.9,
                            "first_pitch_swing_pct": 40.0,
                            "early_o_contact_pct": 50.0,
                            "late_o_contact_pct": 50.0,
                            "armside_pct": 21.4,
                            "gloveside_pct": 56.1,
                            "inside_pct": 15.3,
                            "outside_pct": 62.2,
                            "high_pct": 30.6,
                            "h_mid_pct": 22.4,
                            "v_mid_pct": 15.3,
                            "low_pct": 54.1,
                            "heart_pct": 3.1,
                            "early_pct": 51.0,
                            "behind_pct": 35.7,
                            "late_pct": 27.6,
                            "zone_pct": 37.8,
                            "non_bip_strike_pct": 50.0,
                            "early_bip_pct": 4.0,
                            "groundball_pct": 75.0,
                            "linedrive_pct": 12.5,
                            "flyball_pct": 25.0,
                            "infield_flyball_pct": 0.0,
                            "weak_pct": 25.0,
                            "medium_pct": 62.5,
                            "hard_pct": 12.5,
                            "pull_pct": 75.0,
                            "opposite_field_pct": 12.5,
                            "swing_pct": 42.9,
                            "o_swing_pct": 40.5,
                            "z_swing_pct": 59.5,
                            "contact_pct": 61.9,
                            "o_contact_pct": 47.1,
                            "z_contact_pct": 72.0,
                            "called_strike_pct": 15.3,
                            "early_called_strike_pct": 18.0,
                            "late_o_swing_pct": 33.3,
                            "f_strike_pct": 52.2,
                            "true_f_strike_pct": 47.8,
                            "put_away_pct": 33.3,
                            "batting_avg": 0.1,
                            "walk_pct": 13.0,
                            "bacon_pct": 0.2,
                            "on_base_pct": 0.217,
                            "whip_pct": 1.0
                        }
                    }
                }
            }
        }
    }
}"""
                output = json.loads(mockjson)
            
                return output


                # Set up columnar data for local browser storage and filters
                # Front end can quickly slice on lookup of index in game_id_index data hash
                start = results['start'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
                win = results['win'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
                loss = results['loss'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
                save = results['save'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
                hold = results['hold'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
                ip = results['ip'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
                er = results['er'].to_numpy(dtype=int,copy=True,na_value=0).tolist()

                # Convert datetime to usable json format
                results['game-date'] = pd.to_datetime(results['game-date']).dt.strftime("%a %m/%d/%Y")

                output_dict = { 'player_id': player_id, 'is_pitcher': self.is_pitcher, 'is_active': self.is_active, 'data': { 'game_id_index':{}, 'start': start, 'win': win, 'loss': loss, 'save': save, 'hold': hold, 'ip': ip, 'hits': hits, 'r': r, 'er': er, 'bb': bb, 'k': k, 'pitch_count': pitch_count, 'pa': pa, 'ab': ab, 'hbp': hbp, 'hr': hr, 'flyball': fb, 'sac': sac, 'whiff': whiff, 'csw': csw }, 'logs': {} }

                # Drop cols that are not displayed on the front end
                results.drop(columns=['start','pa','ab','sac','csw'], inplace=True)
                
                # Ensure we have valid data for NaN entries using json.dumps of Python None object
                result_dict = json.loads(results.to_json(orient='index'))
                index = 0
                
                for key, value in result_dict.items():
                    output_dict['data']['game_id_index'][key] = index
                    output_dict['logs'][key] = value
                    output_dict['logs'][key]['index'] = index
                    index += 1

                return output_dict
            else:
                rbi = results['rbi'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
                bases = results['total-bases'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
                barrel = results['barrel'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
                sb = results['sb'].to_numpy(dtype=int,copy=True,na_value=0).tolist()
                cs = results['cs'].to_numpy(dtype=int,copy=True,na_value=0).tolist()

                # Convert datetime to usable json format
                results['game-date'] = pd.to_datetime(results['game-date']).dt.strftime("%a %m/%d/%Y")

                output_dict = { 'player_id': player_id, 'is_pitcher': self.is_pitcher, 'is_active': self.is_active, 'data': { 'game_id_index':{}, 'hits': hits, 'r': r, 'bb': bb, 'k': k, 'sb': sb, 'cs': cs, 'rbi': rbi, 'total-bases': bases, 'barrel': barrel, 'pitch_count': pitch_count, 'pa': pa, 'ab': ab, 'hbp': hbp, 'hr': hr, 'flyball': fb, 'sac': sac, 'whiff': whiff, 'csw': csw }, 'logs': {} }
                
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

