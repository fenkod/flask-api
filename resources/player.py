from flask import current_app
from flask_restful import Resource
from sqlalchemy import false, true
from helpers import fetch_dataframe, var_dump
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
        self.primary_position = ''
        self.hitter_depth_chart_position = ''
        self.pitcher_depth_chart_position = ''

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
            self.player_id = int(player_id)

            player_info = self.fetch_result('bio', player_id)
            if player_info:
                self.first_name = player_info[0]['name_first']
                self.last_name = player_info[0]['name_last']
                self.dob = player_info[0]['birth_date']
                self.is_pitcher = bool(player_info[0]['is_pitcher'])
                self.is_active = bool(player_info[0]['is_active'])
                self.primary_position = player_info[0]['primary_position']
                self.hitter_depth_chart_position = player_info[0]['hitter_depth_chart_position']
                self.pitcher_depth_chart_position = player_info[0]['pitcher_depth_chart_position']
            


            self.career_stats = self.fetch_result('career', player_id)

        return self.fetch_result(query_type, player_id)

    
    def fetch_result(self, query_type, player_id):
        # Caching wrapper for fetch_data
        result = None

        if (current_app.config.get('BYPASS_CACHE')):
            # Bypassing Caching of JSON Results
            result = self.fetch_data(query_type, player_id)
        else:
            # Using Cache for JSON Results
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
        query_var=None
        if (type(player_id) is int):
            query_var = player_id

        # If running player rankings queries, have to run multiple queries. Need to go down a different path here
        if(query_type == "ranks"):
            return self.fetch_ranks_data(query_type, player_id)
        # Do not allow query to go through if attempting to hit the helper functions
        elif(query_type == 'startingpitcherpoolrankingslookup' 
            or query_type == 'startingpitchercustomrankings'
            or query_type == 'startingpitcherpitchpoolrankingslookup'
            or query_type == 'startingpitcherpitchcustomrankings'
            or query_type == 'reliefpitcherpoolrankingslookup' 
            or query_type == 'reliefpitchercustomrankings'
            or query_type == 'reliefpitcherpitchpoolrankingslookup'
            or query_type == 'reliefpitcherpitchcustomrankings'
            or query_type == 'hitterpoolrankingslookup'):
            query = self.get_query('default', player_id)

            raw = fetch_dataframe(query,query_var)
            results = self.format_results(query_type, raw)
            output = self.get_json(query_type,player_id,results)

            return output
        # Otherwise, use default query mapping
        else:
            query = self.get_query(query_type, player_id)


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
                        f'year::text,'
                        f'g::int,'
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
            sql_query = ''

            table_select =  (f'select 	players.mlb_player_id as "mlbamid",'
                                        f'players.full_name as "playername",'
                                        f'cast(current_team.team_id as int4) as "teamid",'
                                        f'current_team.abbreviation as "team",'
                                        f'last_game.game_date as "lastgame",'
                                        f'case when players.primary_position in (\'SP\', \'RP\', \'P\') then 1 else 0 end as "is_pitcher",'
                                        f'case when players.status in (\'A\') then 1 else 0 end as "is_active",'
                                        f'players.first_name as "name_first",'
                                        f'players.last_name as "name_last",'
                                        f'players.birth_date as "birth_date",'
                                        f'players.primary_position as "primary_position",'
                                        f'players.status as "status",'
                                        f'players.batting_hand as "batting_hand",'
                                        f'players.throwing_hand as "throwing_hand",'
                                        f'players.birth_city as "birth_city",'
                                        f'players.birth_country as "birth_country",'
                                        f'players.birth_state as "birth_state",'
                                        f'players.college as "college",'
                                        f'players.high_school as "high_school",'
                                        f'cast(players.pro_debut_date as date) as "pro_debut_date",'
                                        f'players.draft_round as "draft_round",'
                                        f'players.draft_pick as "draft_pick",'
                                        f'players.draft_year as "draft_year",'
                                        f'draft_team.team_id as "draft_team_id",'
                                        f'draft_team.abbreviation as "draft_team",'
                                        f'players.jersey_number as "jersey_number",'
                                        f'players.height as "height",'
                                        f'players.weight as "weight",'
                                        f'highest_hitter_depth_chart_position.position as hitter_depth_chart_position,'
                                        f'highest_hitter_depth_chart_position.depth as hitter_depth_chart_depth,'
                                        f'highest_pitcher_depth_chart_position.position as pitcher_depth_chart_position,'
                                        f'highest_pitcher_depth_chart_position.depth as pitcher_depth_chart_depth,'
                                        f'cast(players.updated as date) as "updated",'
                                        f'case when last_game.game_date > cast(players.updated as date) then last_game.game_date else cast(players.updated as date) end as "last_update" '
                                f'from players '
                                f'left join teams as current_team on current_team.team_id = players.current_team_id '
                                f'left join teams as draft_team on draft_team.team_id = players.draft_team_id '
                                f'left join lateral (select * from depth_charts where depth_charts.player_id = players.player_id and depth_charts.team_id = current_team.team_id and depth_charts."position" not in (\'SP\', \'BP\', \'CL\') order by depth_charts."depth" fetch first row only) highest_hitter_depth_chart_position on true '
                                f'left join lateral (select * from depth_charts where depth_charts.player_id = players.player_id and depth_charts.team_id = current_team.team_id and depth_charts."position" in (\'SP\', \'BP\', \'CL\') order by depth_charts."depth" fetch first row only) highest_pitcher_depth_chart_position on true '
                                f'left join lateral (select games.game_id from pitching_game_log inner join games on games.game_id = pitching_game_log.game_id where pitching_game_log.player_id = players.player_id order by games.game_date desc fetch first row only) last_pitching_game on true '
                                f'left join lateral (select games.game_id from hitting_game_log inner join games on games.game_id = hitting_game_log.game_id where hitting_game_log.player_id = players.player_id order by games.game_date desc fetch first row only) last_hitting_game on true '
                                f'left join lateral (select games.game_id from fielding_game_log inner join games on games.game_id = fielding_game_log.game_id where fielding_game_log.player_id = players.player_id order by games.game_date desc fetch first row only) last_fielding_game on true '
                                f'left join lateral (select games.game_date from games where games.game_id in (last_pitching_game.game_id, last_hitting_game.game_id, last_fielding_game.game_id) order by games.game_date desc fetch first row only) last_game on true \n ')
            player_select = ''

            if player_id != 'NA':
                player_select = 'WHERE players.mlb_player_id = %s'

            order_by = 'order by players.last_name'

            sql_query = table_select + player_select + order_by

            return sql_query

        def career():
            if (self.is_pitcher):
                return (
                    f'SELECT year::text AS "year", '
                        f'g::int, '
                        f'gs::int, '
                        f'w::int, '
                        f'l::int, '
                        f'sv::int, '
                        f'hld::int, '
                        f'ip, '
                        f'cg::int, '
                        f'sho::int, '
                        f'runs::int, '
                        f'unearned_runs::int, '
                        f'earned_runs::int, '
                        f'era, '
                        f'whip, '
                        f'lob_pct, '
                        f'k_pct, '
                        f'bb_pct, '
                        f'hr_flyball_pct, '
                        f'hbp::int, '
                        f'wp::int, '
                        f'teams,'
                        f'qs::int,'
                        f'pa::int,'
                        f'ip_in_starts as "ip-in-starts",'
                        f'ip_in_relief as "ip-in-relief",'
                        f'outs_in_starts::int as "outs-in-starts",'
                        f'outs_in_relief::int as "outs-in-relief",'
                        f'x_era as "x-era",'
                        f'fip,'
                        f'x_fip as "x-fip",'
                        f'pitch_count::int as "pitch-count",'
                        f'pitch_count_in_starts::int as "pitch-count-in-starts",'
                        f'pitch_count_in_relief::int as "pitch-count-in-relief",'
                        f'hits::int,'
                        f'innings_per_start as "innings-per-start",'
                        f'innings_per_relief as "innings-per-relief",'
                        f'innings_per_game as "innings-per-game",'
                        f'pitches_per_start as "pitches-per-start",'
                        f'pitches_per_relief as "pitches-per-relief",'
                        f'pitches_per_game as "pitches-per-game",'
                        f'hits_per_nine as "hits-per-nine" '
                    f'FROM mv_pitcher_career_stats '
                    f'INNER JOIN players on players.player_id = mv_pitcher_career_stats.pitcher_id '
                    f'WHERE players.mlb_player_id = %s '
                    f'ORDER BY year ASC;'
                )
            else:
                return (
                    f'SELECT year::text AS "year", '
                        f'g::int,'
                        f'gs::int,'
                        f'runs::int, '
                        f'rbi::int, '
                        f'sb::int, '
                        f'cs::int, '
                        f'teams '
                    f'FROM mv_hitter_career_stats '
                    f'INNER JOIN players on players.player_id = mv_hitter_career_stats.hitter_id '
                    f'WHERE players.mlb_player_id = %s '
                    f'ORDER BY year ASC;'
                )

        def gamelogs():
            if (self.is_pitcher):
                return (
                    f'SELECT games.mlb_game_id AS "gameid",'
                            f'game_played AS "game-date",'
                            f'team,'
                            f'pitcherteam.mlb_team_id::int AS "team-id",'
                            f'opponent,'
                            f'opponentteam.mlb_team_id::int AS "opponent-team-id",'
                            f'park,'
                            f'team_result AS "team-result",'
                            f'runs_scored::int AS "runs-scored",'
                            f'opponent_runs_scored::int AS "opponent-runs-scored",'
                            f'mv_pitcher_game_stats.start::int AS "gs",'
                            f'mv_pitcher_game_stats.play ::int AS "g",'
                            f'mv_pitcher_game_stats.complete::int AS "cg",'
                            f'mv_pitcher_game_stats.win::int AS "w",'
                            f'mv_pitcher_game_stats.loss::int AS "l",'
                            f'mv_pitcher_game_stats.save::int AS "sv",'
                            f'mv_pitcher_game_stats.blown_save::int AS "bsv",'
                            f'mv_pitcher_game_stats.hold::int AS "hld",'
                            f'mv_pitcher_game_stats.qstart::int AS "qs",'
                            f'mv_pitcher_game_stats.shutout ::int AS "sho",'
                            f'mv_pitcher_game_stats.ip AS "ip",'
                            f'0::int AS "outs",'
                            f'mv_pitcher_game_stats.runs::int AS "runs",'
                            f'mv_pitcher_game_stats.earned_runs::int AS "earned_runs",'
                            f'mv_pitcher_game_stats.lob::int,'
                            f'mv_pitcher_game_stats.lob_pct,'
                            f'mv_pitcher_game_stats.era,'
                            f'mv_pitcher_game_stats.whip,'
                            f'mv_pitcher_game_stats.x_era as "x-era",'
                            f'pitchtype,'
                            f'opponent_handedness AS "split-RL",'
                            f'avg_velocity AS "velo_avg",'
                            f'strikeout_pct,'
                            f'mv_pitcher_game_logs.bb_pct,'
                            f'usage_pct,'
                            f'batting_average AS "batting_avg",' 
                            f'o_swing_pct,'
                            f'z_swing_pct,'
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
                            f'mv_pitcher_game_logs.hr_flyball_pct,'
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
                            f'mv_pitcher_game_logs.num_pitches::int AS "pitch-count",'
                            f'num_hit::int AS "hits",'
                            f'num_bb::int AS "bb",'
                            f'num_1b::int AS "1b",' 
                            f'num_2b::int AS "2b",' 
                            f'num_3b::int AS "3b",' 
                            f'num_hr::int AS "hr",' 
                            f'num_k::int AS "k",'
                            f'num_pa::int AS "pa",'
                            f'num_strikes::int AS "strikes",' 
                            f'num_balls::int AS "balls",' 
                            f'num_foul::int AS "foul",' 
                            f'num_ibb::int AS "ibb",'
                            f'num_hbp::int AS "hbp",' 
                            f'num_wp::int AS "wp",'
                            f'num_flyball::int as "flyball",'
                            f'num_whiff::int as "whiff",'
                            f'num_zone::int as "zone",'
                            f'total_velo,'
                            f'num_velo::int as "pitches-clocked",'
                            f'num_armside::int as "armside",'
                            f'num_gloveside::int as "gloveside",'
                            f'num_inside::int as "inside",'
                            f'num_outside::int as "outside",'
                            f'num_horizontal_middle::int as "h-mid",'
                            f'num_high::int as "high",'
                            f'num_middle::int as "mid",'
                            f'num_low::int as "low",'
                            f'num_heart::int as "heart",'
                            f'num_early::int as "early",'
                            f'num_late::int as "late",'
                            f'num_behind::int as "behind",'
                            f'num_non_bip_strike::int as "non-bip-strike",'
                            f'num_batted_ball_event::int as "batted-ball-event",'
                            f'num_early_bip::int as "early-bip",'
                            f'num_fastball::int as "fastball",'
                            f'num_secondary::int as "secondary",'
                            f'num_early_secondary::int as "early-secondary",'
                            f'num_late_secondary::int as "late-secondary",'
                            f'num_called_strike::int as "called-strike",'
                            f'num_early_called_strike::int as "early-called-strike",'
                            f'num_put_away::int as "putaway",'
                            f'num_swing::int as "swings",'
                            f'num_contact::int as "contact",'
                            f'num_first_pitch_swing::int as "first-pitch-swings",'
                            f'num_first_pitch_strike::int "first-pitch-strikes",'
                            f'num_true_first_pitch_strike::int as "true-first-pitch-strikes",'
                            f'num_plus_pitch::int as "plus-pitch",'
                            f'num_z_swing::int as "z-swing",'
                            f'num_z_contact::int as "z-contact",'
                            f'num_o_swing::int as "o-swing",'
                            f'num_o_contact::int as "o-contact",'
                            f'num_early_o_swing as "early-o-swing",'
                            f'num_early_o_contact as "early-o-contact",'
                            f'num_late_o_swing::int as "late-o-swing",'
                            f'num_late_o_contact::int as "late-o-contact",'
                            f'num_pulled_bip::int as "pulled-bip",'
                            f'num_opposite_bip::int as "opp-bip",'
                            f'num_line_drive::int as "line-drive",'
                            f'num_if_fly_ball::int as "if-flyball",'
                            f'num_ground_ball::int as "groundball",'
                            f'num_weak_bip::int as "weak-bip",'
                            f'num_medium_bip::int "medium-bip",'
                            f'num_hard_bip::int as "hard-bip",'
                            f'num_ab::int as "ab",'
                            f'games.year_played as "year",'
                            f'games."game_type" as "game-type",'
                            f'num_topped::int as "top-bip",'
                            f'num_under::int as "under-bip",'
                            f'num_flare_or_burner::int as "flare-burner-bip",'
                            f'num_solid::int as "solid-bip",'
                            f'num_barrel::int as "barrel-bip",'
                            f'num_sweet_spot::int as "sweet-spot-bip",'
                            f'num_launch_speed::int as "total-exit-velo",'
                            f'num_launch_angle::int as "total-launch-angle",'
                            f'num_ideal::int as "ideal-bip",'
                            f'num_x_movement::int as "total-x-movement",'
                            f'num_y_movement::int as "total-y-movement",'
                            f'num_x_release::int as "total-x-release",'
                            f'num_y_release::int as "total-y-release",'
                            f'num_pitch_extension::int as "total-pitch-extension",'
                            f'num_spin_rate::int as "total-spin-rate",'
                            f'spin_rate_counter::int as "spin-rates-tracked",'
                            f'topped_pct as "top-pct",'
                            f'under_pct as "under-pct",'
                            f'flare_or_burner_pct as "flare-burner-pct",'
                            f'solid_pct as "solid-pct",'
                            f'barrel_pct as "barrel-pct",'
                            f'sweet_spot_pct as "sweet-spot-pct",'
                            f'average_launch_speed as "exit-velo-avg",'
                            f'average_launch_angle as "launch-angle-avg",'
                            f'ideal_bbe_pct as "ideal-bbe-pct",'
                            f'ideal_pa_pct as "ideal-pa-pct",'
                            f'avg_x_movement as "x-movement-avg",'
                            f'avg_y_movement as "y-movement-avg",'
                            f'avg_x_release as "x-release-avg",'
                            f'avg_y_release as "y-release-avg",'
                            f'avg_pitch_extension as "pitch-extension-avg",'
                            f'avg_spin_rate as "spin-rate-avg",'
                            f'inside_pct as "inside-pct",'
                            f'outside_pct as "outside-pct",'
                            f'fastball_pct as "fastball-pct",'
                            f'secondary_pct as "secondary-pct",'
                            f'early_secondary_pct as "early-secondary-pct",'
                            f'late_secondary_pct as "late-secondary-pct",'
                            f'put_away_pct as "put-away-pct",'
                            f'whiff_pct as "whiff-pct",'
                            f'slug_pct as "slug-pct",'
                            f'on_base_pct as "on-base-pct",'
                            f'ops_pct as "ops-pct",'
                            f'woba_pct as "woba-pct",'
                            f'x_avg as "x-avg",'
                            f'x_slug_pct as "x-slug-pct",'
                            f'x_babip as "x-babip-pct",'
                            f'x_woba as "x-woba-pct",'
                            f'x_wobacon as "x-wobacon-pct",'
                            f'num_center_bip as "center-bip",'
                            f'mv_pitcher_game_logs.num_outs as "bip-outs",'
                            f'num_fly_ball_launch_speed as "total-flyball-exit-velo",'
                            f'average_fly_ball_launch_speed as "flyball-exit-velo-avg" '
                        f'FROM mv_pitcher_game_logs '
                        f'inner join players on players.player_id = mv_pitcher_game_logs.pitcher_id '
                        f'inner join games on games.game_id = mv_pitcher_game_logs.game_id '
                        f'inner join teams as pitcherteam on pitcherteam.team_id = mv_pitcher_game_logs.pitcher_team_id '
                        f'inner join teams as opponentteam on opponentteam.team_id = mv_pitcher_game_logs.opponent_team_id '
                        f'inner join mv_pitcher_game_stats on mv_pitcher_game_stats.game_id = mv_pitcher_game_logs.game_id and mv_pitcher_game_stats.pitcher_id = players.player_id '
                        f'WHERE players.mlb_player_id = %s '
                        f'ORDER BY mv_pitcher_game_logs.year_played DESC, mv_pitcher_game_logs.month_played DESC, mv_pitcher_game_logs.game_played DESC;'
                )
            else:
                return (
                    f'SELECT games.mlb_game_id AS "gameid",'
                            f'game_played AS "game-date",'
                            f'team,'
                            f'hitterteam.mlb_team_id::int AS "team-id",'
                            f'opponent,'
                            f'opponentteam.mlb_team_id::int AS "opponent-team-id",'
                            f'park,'
                            f'team_result AS "team-result",'
                            f'runs_scored::int AS "runs-scored",'
                            f'opponent_runs_scored::int AS "opponent-runs-scored",'
                            f'gs::int,'
                            f'g::int,'
                            f'sb::int,'
                            f'cs::int,'
                            f'rbi::int,'
                            f'runs::int,'
                            f'lob::int,'
                            f'pitchtype,'
                            f'opponent_handedness AS "split-RL",'
                            f'avg_velocity AS "velo_avg",'
                            f'strikeout_pct,'
                            f'bb_pct,'
                            f'usage_pct,'
                            f'batting_average AS "batting_avg",' 
                            f'o_swing_pct,'
                            f'z_swing_pct,'
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
                            f'num_pitches::int AS "pitch-count",'
                            f'num_hit::int AS "hits",'
                            f'num_bb::int AS "bb",'
                            f'num_1b::int AS "1b",' 
                            f'num_2b::int AS "2b",' 
                            f'num_3b::int AS "3b",' 
                            f'num_hr::int AS "hr",' 
                            f'num_k::int AS "k",'
                            f'num_pa::int AS "pa",'
                            f'num_strikes::int AS "strikes",' 
                            f'num_balls::int AS "balls",' 
                            f'num_foul::int AS "foul",' 
                            f'num_ibb::int AS "ibb",'
                            f'num_hbp::int AS "hbp",' 
                            f'num_wp::int AS "wp",'
                            f'num_flyball::int as "flyball",'
                            f'num_whiff::int as "whiff",'
                            f'num_zone::int as "zone",'
                            f'total_velo,'
                            f'num_velo::int as "pitches-clocked",'
                            f'num_armside::int as "armside",'
                            f'num_gloveside::int as "gloveside",'
                            f'num_inside::int as "inside",'
                            f'num_outside::int as "outside",'
                            f'num_horizontal_middle::int as "h-mid",'
                            f'num_high::int as "high",'
                            f'num_middle::int as "mid",'
                            f'num_low::int as "low",'
                            f'num_heart::int as "heart",'
                            f'num_early::int as "early",'
                            f'num_late::int as "late",'
                            f'num_behind::int as "behind",'
                            f'num_non_bip_strike::int as "non-bip-strike",'
                            f'num_batted_ball_event::int as "batted-ball-event",'
                            f'num_early_bip::int as "early-bip",'
                            f'num_fastball::int as "fastball",'
                            f'num_secondary::int as "secondary",'
                            f'num_early_secondary::int as "early-secondary",'
                            f'num_late_secondary::int as "late-secondary",'
                            f'num_called_strike::int as "called-strike",'
                            f'num_early_called_strike::int as "early-called-strike",'
                            f'num_put_away::int as "putaway",'
                            f'num_swing::int as "swings",'
                            f'num_contact::int as "contact",'
                            f'num_first_pitch_swing::int as "first-pitch-swings",'
                            f'num_first_pitch_strike::int "first-pitch-strikes",'
                            f'num_true_first_pitch_strike::int as "true-first-pitch-strikes",'
                            f'num_plus_pitch::int as "plus-pitch",'
                            f'num_z_swing::int as "z-swing",'
                            f'num_z_contact::int as "z-contact",'
                            f'num_o_swing::int as "o-swing",'
                            f'num_o_contact::int as "o-contact",'
                            f'num_early_o_swing as "early-o-swing",'
                            f'num_early_o_contact as "early-o-contact",'
                            f'num_late_o_swing::int as "late-o-swing",'
                            f'num_late_o_contact::int as "late-o-contact",'
                            f'num_pulled_bip::int as "pulled-bip",'
                            f'num_opposite_bip::int as "opp-bip",'
                            f'num_line_drive::int as "line-drive",'
                            f'num_if_fly_ball::int as "if-flyball",'
                            f'num_ground_ball::int as "groundball",'
                            f'num_weak_bip::int as "weak-bip",'
                            f'num_medium_bip::int "medium-bip",'
                            f'num_hard_bip::int as "hard-bip",'
                            f'num_ab::int as "ab",'
                            f'games.year_played as "year",'
                            f'games."game_type" as "game-type",'
                            f'num_topped::int as "top-bip",'
                            f'num_under::int as "under-bip",'
                            f'num_flare_or_burner::int as "flare-burner-bip",'
                            f'num_solid::int as "solid-bip",'
                            f'num_barrel::int as "barrel-bip",'
                            f'num_sweet_spot::int as "sweet-spot-bip",'
                            f'num_launch_speed::int as "total-exit-velo",'
                            f'num_launch_angle::int as "total-launch-angle",'
                            f'num_ideal::int as "ideal-bip",'
                            f'num_x_movement::int as "total-x-movement",'
                            f'num_y_movement::int as "total-y-movement",'
                            f'num_x_release::int as "total-x-release",'
                            f'num_y_release::int as "total-y-release",'
                            f'num_pitch_extension::int as "total-pitch-extension",'
                            f'num_spin_rate::int as "total-spin-rate",'
                            f'spin_rate_counter::int as "spin-rates-tracked",'
                            f'topped_pct as "top-pct",'
                            f'under_pct as "under-pct",'
                            f'flare_or_burner_pct as "flare-burner-pct",'
                            f'solid_pct as "solid-pct",'
                            f'barrel_pct as "barrel-pct",'
                            f'sweet_spot_pct as "sweet-spot-pct",'
                            f'average_launch_speed as "exit-velo-avg",'
                            f'average_launch_angle as "launch-angle-avg",'
                            f'ideal_bbe_pct as "ideal-bbe-pct",'
                            f'ideal_pa_pct as "ideal-pa-pct",'
                            f'avg_x_movement as "x-movement-avg",'
                            f'avg_y_movement as "y-movement-avg",'
                            f'avg_x_release as "x-release-avg",'
                            f'avg_y_release as "y-release-avg",'
                            f'avg_pitch_extension as "pitch-extension-avg",'
                            f'avg_spin_rate as "spin-rate-avg",'
                            f'inside_pct as "inside-pct",'
                            f'outside_pct as "outside-pct",'
                            f'fastball_pct as "fastball-pct",'
                            f'secondary_pct as "secondary-pct",'
                            f'early_secondary_pct as "early-secondary-pct",'
                            f'late_secondary_pct as "late-secondary-pct",'
                            f'put_away_pct as "put-away-pct",'
                            f'whiff_pct as "whiff-pct",'
                            f'slug_pct as "slug-pct",'
                            f'on_base_pct as "on-base-pct",'
                            f'ops_pct as "ops-pct",'
                            f'woba_pct as "woba-pct",'
                            f'x_avg as "x-avg",'
                            f'x_slug_pct as "x-slug-pct",'
                            f'x_babip as "x-babip-pct",'
                            f'x_woba as "x-woba-pct",'
                            f'x_wobacon as "x-wobacon-pct",'
                            f'num_center_bip as "center-bip",'
                            f'num_outs as "bip-outs",'
                            f'num_fly_ball_launch_speed as "total-flyball-exit-velo",'
                            f'average_fly_ball_launch_speed as "flyball-exit-velo-avg",'
                            f'num_xbh as xbh,'
                            f'max_launch_speed as "max-exit-velo",'
                            f'batting_order_position as "batting-order-position"'
                        f'FROM mv_hitter_game_logs '
                        f'inner join players on players.player_id = mv_hitter_game_logs.hitter_id '
                        f'inner join games on games.game_id = mv_hitter_game_logs.game_id '
                        f'inner join teams as hitterteam on hitterteam.team_id = mv_hitter_game_logs.hitter_team_id '
                        f'inner join teams as opponentteam on opponentteam.team_id = mv_hitter_game_logs.opponent_team_id '
                        f'WHERE players.mlb_player_id = %s '
                        f'ORDER BY mv_hitter_game_logs.year_played DESC, mv_hitter_game_logs.month_played DESC, mv_hitter_game_logs.game_played DESC;'

                    #and game_played >= current_date - interval \'400 day\'
                )

        def locationlogs():
            if (self.is_pitcher):
                return (
                    f'SELECT DISTINCT games.mlb_game_id AS "gameid", ' 
                            f'pitchtype, '
                            f'hitterside AS "split-RL", '
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
                            f'avg_velocity,'
                            f'bbe as "batted-ball-event",'
                            f'outs,'
                            f'avg_x_movement as "x-movement-avg",'
                            f'avg_y_movement as "y-movement-avg",'
                            f'avg_spin_rate as "spin-rate-avg" '
                        f'FROM mv_pitcher_game_log_pitches '
                        f'inner join players on players.player_id = mv_pitcher_game_log_pitches.pitcher_id '
                        f'inner join games on games.game_id = mv_pitcher_game_log_pitches.game_id '
                        f'WHERE players.mlb_player_id = %s '
                        f'ORDER BY games.mlb_game_id;'
                )
            else:
                return (
                    f'SELECT DISTINCT games.mlb_game_id AS "gameid",'
                            f'pitchtype, '
                            f'pitcherside AS "split-RL", '
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
                            f'avg_velocity,'
                            f'bbe as "batted-ball-event",'
                            f'outs,'
                            f'avg_x_movement as "x-movement-avg",'
                            f'avg_y_movement as "y-movement-avg",'
                            f'avg_spin_rate as "spin-rate-avg" '
                    f'FROM mv_hitter_game_log_pitches '
                    f'inner join players on players.player_id = mv_hitter_game_log_pitches.hitter_id '
                    f'inner join games on games.game_id = mv_hitter_game_log_pitches.game_id '
                    f'WHERE players.mlb_player_id = %s '
                    f'ORDER BY games.mlb_game_id;'
                )

        def locations():
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
            table_select =  (f'SELECT players.mlb_player_id as id, '
                                f'players.full_name as name, '
                                f'json_agg(DISTINCT jsonb_build_object(pp.game_year, (SELECT json_object_agg(pp2.position, pp2.games_played) FROM pl_playerpositions pp2 WHERE pp2.mlb_player_id = pp.mlb_player_id AND pp2.game_year = pp.game_year))) as positions '
                                f'FROM players '
                                f'INNER JOIN pl_playerpositions pp '
                                f'USING(mlb_player_id)\n')
            player_select = ''
            group_by = 'GROUP BY players.mlb_player_id, players.full_name'

            if player_id != 'NA':
                player_select = 'WHERE players.mlb_player_id = %s'

            sql_query = table_select + player_select + group_by

            return sql_query

        def stats():
            if (self.is_pitcher):
                return (
                    f'SELECT 	pitchtype, ' 
                                f'year_played::text AS "year", ' 
                                f'opponent_handedness AS "split-RL", '
                                f'home_away AS "split-HA", '
                                f'avg_velocity AS "velo_avg", '
                                f'k_pct, '
                                f'bb_pct, '
                                f'usage_pct, '
                                f'batting_average AS "batting_avg", ' 
                                f'o_swing_pct, '
                                f'zone_pct, '
                                f'swinging_strike_pct, '
                                f'called_strike_pct, '
                                f'csw_pct, '
                                f'cswf_pct, '
                                f'plus_pct, '
                                f'foul_pct, '
                                f'contact_pct, '
                                f'o_contact_pct, '
                                f'z_contact_pct, '
                                f'swing_pct, '
                                f'strike_pct, '
                                f'early_called_strike_pct, '
                                f'late_o_swing_pct, '
                                f'f_strike_pct, '
                                f'true_f_strike_pct, '
                                f'groundball_pct, '
                                f'linedrive_pct, '
                                f'flyball_pct, '
                                f'hr_flyball_pct, '
                                f'groundball_flyball_pct, '
                                f'infield_flyball_pct, '
                                f'weak_pct, '
                                f'medium_pct, '
                                f'hard_pct, '
                                f'center_pct, '
                                f'pull_pct, '
                                f'opposite_field_pct, '
                                f'babip_pct, '
                                f'bacon_pct, '
                                f'armside_pct, '
                                f'gloveside_pct, '
                                f'vertical_middle_location_pct AS "v_mid_pct", '
                                f'horizonal_middle_location_pct AS "h_mid_pct", '
                                f'high_pct, '
                                f'low_pct, '
                                f'heart_pct, '
                                f'early_pct, '
                                f'behind_pct, '
                                f'late_pct, '
                                f'non_bip_strike_pct, '
                                f'early_bip_pct, '
                                f'num_pitches::int AS "pitch-count", ' 
                                f'num_hits::int AS "hits", ' 
                                f'num_bb::int AS "bb", ' 
                                f'num_1b::int AS "1b", ' 
                                f'num_2b::int AS "2b", '
                                f'num_3b::int AS "3b", ' 
                                f'num_hr::int AS "hr", ' 
                                f'num_k::int AS "k", '
                                f'num_pa::int AS "pa", '
                                f'num_strike::int AS "strikes", ' 
                                f'num_ball::int AS "balls", ' 
                                f'num_foul::int AS "foul", ' 
                                f'num_ibb::int AS "ibb", ' 
                                f'num_hbp::int AS "hbp", ' 
                                f'num_wp::int AS "wp", '
                                f'num_fastball AS "fastball",'
                                f'num_secondary AS "secondary",'
                                f'num_inside AS "inside",'
                                f'num_outside AS "outside",'
                                f'num_early_secondary as "early-secondary",'
                                f'num_late_secondary as "late-secondary",'
                                f'num_put_away as "putaway",'
                                f'num_topped::int as "top-bip",'
                                f'num_under::int as "under-bip",'
                                f'num_flare_or_burner::int as "flare-burner-bip",'
                                f'num_solid::int as "solid-bip",'
                                f'num_barrel::int as "barrel-bip",'
                                f'num_sweet_spot::int as "sweet-spot-bip",'
                                f'num_launch_speed::int as "total-exit-velo",'
                                f'num_launch_angle::int as "total-launch-angle",'
                                f'num_ideal::int as "ideal-bip",'
                                f'num_x_movement::int as "total-x-movement",'
                                f'num_y_movement::int as "total-y-movement",'
                                f'num_x_release::int as "total-x-release",'
                                f'num_y_release::int as "total-y-release",'
                                f'num_pitch_extension::int as "total-pitch-extension",'
                                f'num_spin_rate::int as "total-spin-rate",'
                                f'spin_rate_counter::int as "spin-rates-tracked",'
                                f'topped_pct as "top-pct",'
                                f'under_pct as "under-pct",'
                                f'flare_or_burner_pct as "flare-burner-pct",'
                                f'solid_pct as "solid-pct",'
                                f'barrel_pct as "barrel-pct",'
                                f'sweet_spot_pct as "sweet-spot-pct",'
                                f'average_launch_speed as "exit-velo-avg",'
                                f'average_launch_angle as "launch-angle-avg",'
                                f'ideal_bbe_pct as "ideal-bbe-pct",'
                                f'ideal_pa_pct as "ideal-pa-pct",'
                                f'avg_x_movement as "x-movement-avg",'
                                f'avg_y_movement as "y-movement-avg",'
                                f'avg_x_release as "x-release-avg",'
                                f'avg_y_release as "y-release-avg",'
                                f'avg_pitch_extension as "pitch-extension-avg",'
                                f'avg_spin_rate as "spin-rate-avg",'
                                f'inside_pct as "inside-pct",'
                                f'outside_pct as "outside-pct",'
                                f'fastball_pct as "fastball-pct",'
                                f'secondary_pct as "secondary-pct",'
                                f'early_secondary_pct as "early-secondary-pct",'
                                f'late_secondary_pct as "late-secondary-pct",'
                                f'put_away_pct as "put-away-pct",'
                                f'whiff_pct as "whiff-pct",'
                                f'slug_pct as "slug-pct",'
                                f'on_base_pct as "on-base-pct",'
                                f'ops_pct as "ops-pct",'
                                f'woba_pct as "woba-pct",'
                                f'x_avg as "x-avg",'
                                f'x_slug_pct as "x-slug-pct",'
                                f'x_babip as "x-babip-pct",'
                                f'x_woba as "x-woba-pct",'
                                f'x_wobacon as "x-wobacon-pct",'
                                f'average_fly_ball_launch_speed as "flyball-exit-velo-avg" '
                            f'FROM player_page_repertoire '
                            f'inner join players on players.player_id = player_page_repertoire.pitcher_id '
                            f'WHERE players.mlb_player_id = %s '
                            f'ORDER BY pitchtype, year_played, opponent_handedness, home_away;'
                )
            else:
                return (
                    f'SELECT 	pitchtype,'
                                f'year_played::text AS "year",' 
                                f'opponent_handedness AS "split-RL",'
                                f'home_away AS "split-HA",'
                                f'avg_velocity AS "velo_avg",'
                                f'k_pct,'
                                f'bb_pct,'
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
                                f'num_pitches::int AS "pitch-count",' 
                                f'num_hits::int AS "hits",' 
                                f'num_bb::int AS "bb",' 
                                f'num_1b::int AS "1b",' 
                                f'num_2b::int AS "2b",'
                                f'num_3b::int AS "3b",' 
                                f'num_hr::int AS "hr",' 
                                f'num_k::int AS "k",'
                                f'num_pa::int AS "pa",'
                                f'num_strike::int AS "strikes",' 
                                f'num_ball::int AS "balls",' 
                                f'num_foul::int AS "foul",' 
                                f'num_ibb::int AS "ibb",' 
                                f'num_hbp::int AS "hbp",' 
                                f'num_wp::int AS "wp",'
                                f'num_fastball AS "fastball",'
                                f'num_secondary AS "secondary",'
                                f'num_inside AS "inside",'
                                f'num_outside AS "outside",'
                                f'num_early_secondary as "early-secondary",'
                                f'num_late_secondary as "late-secondary",'
                                f'num_put_away as "putaway",'
                                f'num_topped::int as "top-bip",'
                                f'num_under::int as "under-bip",'
                                f'num_flare_or_burner::int as "flare-burner-bip",'
                                f'num_solid::int as "solid-bip",'
                                f'num_barrel::int as "barrel-bip",'
                                f'num_sweet_spot::int as "sweet-spot-bip",'
                                f'num_launch_speed::int as "total-exit-velo",'
                                f'num_launch_angle::int as "total-launch-angle",'
                                f'num_ideal::int as "ideal-bip",'
                                f'num_x_movement::int as "total-x-movement",'
                                f'num_y_movement::int as "total-y-movement",'
                                f'num_x_release::int as "total-x-release",'
                                f'num_y_release::int as "total-y-release",'
                                f'num_pitch_extension::int as "total-pitch-extension",'
                                f'num_spin_rate::int as "total-spin-rate",'
                                f'spin_rate_counter::int as "spin-rates-tracked",'
                                f'topped_pct as "top-pct",'
                                f'under_pct as "under-pct",'
                                f'flare_or_burner_pct as "flare-burner-pct",'
                                f'solid_pct as "solid-pct",'
                                f'barrel_pct as "barrel-pct",'
                                f'sweet_spot_pct as "sweet-spot-pct",'
                                f'average_launch_speed as "exit-velo-avg",'
                                f'average_launch_angle as "launch-angle-avg",'
                                f'ideal_bbe_pct as "ideal-bbe-pct",'
                                f'ideal_pa_pct as "ideal-pa-pct",'
                                f'avg_x_movement as "x-movement-avg",'
                                f'avg_y_movement as "y-movement-avg",'
                                f'avg_x_release as "x-release-avg",'
                                f'avg_y_release as "y-release-avg",'
                                f'avg_pitch_extension as "pitch-extension-avg",'
                                f'avg_spin_rate as "spin-rate-avg",'
                                f'inside_pct as "inside-pct",'
                                f'outside_pct as "outside-pct",'
                                f'fastball_pct as "fastball-pct",'
                                f'secondary_pct as "secondary-pct",'
                                f'early_secondary_pct as "early-secondary-pct",'
                                f'late_secondary_pct as "late-secondary-pct",'
                                f'put_away_pct as "put-away-pct",'
                                f'whiff_pct as "whiff-pct",'
                                f'slug_pct as "slug-pct",'
                                f'on_base_pct as "on-base-pct",'
                                f'ops_pct as "ops-pct",'
                                f'woba_pct as "woba-pct",'
                                f'x_avg as "x-avg",'
                                f'x_slug_pct as "x-slug-pct",'
                                f'x_babip as "x-babip-pct",'
                                f'x_woba as "x-woba-pct",'
                                f'x_wobacon as "x-wobacon-pct",'
                                f'average_fly_ball_launch_speed as "flyball-exit-velo-avg",'
                                f'num_xbh as "xbh",'
                                f'max_launch_speed as "max-exit-velo" '
                        f'FROM mv_hitter_page_stats '
                        f'inner join players on players.player_id = mv_hitter_page_stats.hitter_id '
                        f'WHERE players.mlb_player_id = %s '
                        f'ORDER BY pitchtype, year_played, opponent_handedness, home_away;'
                )

        def startingpitcherpoolrankingslookup():
            return (f'select 	mv_pitcher_career_stats."year"::int as "year",'
                               f' mv_starting_pitcher_pool.pitcher_rank as qualified_rank '
                        f'from players '
                        f'inner join mv_pitcher_career_stats on mv_pitcher_career_stats.pitcher_id = players.player_id '
                        f'left join mv_starting_pitcher_pool on mv_starting_pitcher_pool.year_played = mv_pitcher_career_stats."year"::int and mv_starting_pitcher_pool.player_id = mv_pitcher_career_stats.pitcher_id '
                        f'where mv_pitcher_career_stats."year" != \'ALL\' '
                        f'and mv_pitcher_career_stats.gs > 0 '
                        f'and players.mlb_player_id = %s ')

        def startingpitchercustomrankings():
            return (f'select 	g.year_played as "year",'
                                f'g.qualified_rank as "qualified-rank",'
                                f'g.w as "w",'
                                f'g.w_percentile as "w-percentile",'
                                f'g.w_rank as "w-rank",'
                                f'g.ip as "ip",'
                                f'g.ip_percentile as "ip-percentile",'
                                f'g.ip_rank as "ip-rank",'
                                f'g.era as "era",'
                                f'g.era_percentile as "era-percentile",'
                                f'g.era_rank as "era-rank",'
                                f'g.whip as "whip",'
                                f'g.whip_percentile as "whip-percentile",'
                                f'g.whip_rank as "whip-rank",'
                                f'g.k_pct as "k-pct",'
                                f'g.k_pct_percentile as "k-pct-percentile",'
                                f'g.k_pct_rank as "k-pct-rank",'
                                f'g.bb_pct as "bb-pct",'
                                f'g.bb_pct_percentile as "bb-pct-percentile",'
                                f'g.bb_pct_rank as "bb-pct-rank",'
                                f'g.csw_pct as "csw-pct",'
                                f'g.csw_pct_percentile as "csw-pct-percentile",'
                                f'g.csw_pct_rank as "csw-pct-rank",'
                                f'g.swinging_strike_pct as "swinging-strike-pct",'
                                f'g.swinging_strike_pct_percentile as "swinging-strike-pct-percentile",'
                                f'g.swinging_strike_pct_rank as "swinging-strike-pct-rank",'
                                f'g.hard_pct as "hard-pct",'
                                f'g.hard_pct_percentile as "hard-pct-percentile",'
                                f'g.hard_pct_rank as "hard-pct-rank",'
                                f'g.groundball_pct as "groundball-pct",'
                                f'g.groundball_pct_percentile as "groundball-pct-percentile",'
                                f'g.groundball_pct_rank as "groundball-pct-rank",'
                                f'g.x_era as "x-era",'
                                f'g.x_era_percentile as "x-era-percentile",'
                                f'g.x_era_rank as "x-era-rank",'
                                f'g.x_woba as "x-woba",'
                                f'g.x_woba_percentile as "x-woba-percentile",'
                                f'g.x_woba_rank as "x-woba-rank",'
                                f'yearly_averages.w as "league-w",'
                                f'yearly_averages.ip as "league-ip",'
                                f'yearly_averages.era as "league-era",'
                                f'yearly_averages.whip as "league-whip",'
                                f'yearly_averages.k_pct as "league-k-pct",'
                                f'yearly_averages.bb_pct as "league-bb-pct",'
                                f'yearly_averages.csw_pct as "league-csw-pct",'
                                f'yearly_averages.swinging_strike_pct as "league-swinging-strike-pct",'
                                f'yearly_averages.hard_pct as "league-hard-pct",'
                                f'yearly_averages.groundball_pct as "league-groundball-pct",'
                                f'yearly_averages.x_era as "league-x-era",'
                                f'yearly_averages.x_woba as "league-x-woba",'
                                f'yearly_averages.league_w_percentage as "league-w-percentile",'
                                f'yearly_averages.league_ip_percentage as "league-ip-percentile",'
                                f'yearly_averages.league_era_percentage as "league-era-percentile",'
                                f'yearly_averages.league_whip_percentage as "league-whip-percentile",'
                                f'yearly_averages.league_k_pct_percentage as "league-k-pct-percentile",'
                                f'yearly_averages.league_bb_pct_percentage as "league-bb-pct-percentile",'
                                f'yearly_averages.league_csw_pct_percentage as "league-csw-pct-percentile",'
                                f'yearly_averages.league_swinging_strike_pct_percentage as "league-swinging-strike-pct-percentile",'
                                f'yearly_averages.league_hard_pct_percentage as "league-hard-pct-percentile",'
                                f'yearly_averages.league_groundball_pct_percentage as "league-groundball-pct-percentile",'
                                f'yearly_averages.league_x_era_percentage as "league-x-era-percentile",'
                                f'yearly_averages.league_x_woba_percentage as "league-x-woba-percentile" '
                        f'from '
                        f'( '
                            f'SELECT f.year_played,'
                                f'f.pitcher_rank AS qualified_rank,'
                                f'f.w,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.w) AS w_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.w DESC) AS w_rank,'
                                f'f.ip,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.ip) AS ip_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.ip DESC) AS ip_rank,'
                                f'f.era,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.era DESC) AS era_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.era) AS era_rank,'
                                f'f.whip,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.whip DESC) AS whip_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.whip) AS whip_rank,'
                                f'f.k_pct,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.k_pct) AS k_pct_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.k_pct DESC) AS k_pct_rank,'
                                f'f.bb_pct,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.bb_pct DESC) AS bb_pct_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.bb_pct) AS bb_pct_rank,'
                                f'f.csw_pct,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.csw_pct) AS csw_pct_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.csw_pct DESC) AS csw_pct_rank,'
                                f'f.swinging_strike_pct,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.swinging_strike_pct) AS swinging_strike_pct_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.swinging_strike_pct DESC) AS swinging_strike_pct_rank,'
                                f'f.hard_pct,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.hard_pct DESC) AS hard_pct_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.hard_pct) AS hard_pct_rank,'
                                f'f.groundball_pct,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.groundball_pct) AS groundball_pct_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.groundball_pct DESC) AS groundball_pct_rank,'
                                f'f.x_era,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.x_era DESC) AS x_era_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.x_era) AS x_era_rank,'
                                f'f.x_woba,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.x_woba DESC) AS x_woba_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.x_woba) AS x_woba_rank '
                            f'from ' 
                            f' ('
                                f'select 	year_played, '
                                        f'"position",'
                                        f'g,'
                                        f'gs,'
                                        f'w,'
                                        f'l,'
                                        f'sv,'
                                        f'hld,'
                                        f'cg,'
                                        f'sho,'
                                        f'qs,'
                                        f'runs,'
                                        f'earned_runs,'
                                        f'hbp,'
                                        f'wp,'
                                        f'pa,'
                                        f'pitch_count,'
                                        f'pitches_per_game,'
                                        f'ip,'
                                        f'ip_per_game,'
                                        f'era,'
                                        f'whip,'
                                        f'lob_pct,'
                                        f'k_pct,'
                                        f'bb_pct,'
                                        f'fip,'
                                        f'csw_pct,'
                                        f'hard_pct,'
                                        f'groundball_pct,'
                                        f'swinging_strike_pct,'
                                        f'x_era,'
                                        f'x_woba,'
                                        f'pitcher_rank '
                                        f'from mv_starting_pitcher_pool '
                                        f'inner join players on mv_starting_pitcher_pool.player_id = players.player_id '
                                        f'where players.mlb_player_id != %s '
                                f'union '
                                f'select 	year_played, '
                                            f'"position",'
                                            f'g,'
                                            f'gs,'
                                            f'w,'
                                            f'l,'
                                            f'sv,'
                                            f'hld,'
                                            f'cg,'
                                            f'sho,'
                                            f'qs,'
                                            f'runs,'
                                            f'earned_runs,'
                                            f'hbp,'
                                            f'wp,'
                                            f'pa,'
                                            f'pitch_count,'
                                            f'pitches_per_game,'
                                            f'ip,'
                                            f'ip_per_game,'
                                            f'era,'
                                            f'whip,'
                                            f'lob_pct,'
                                            f'k_pct,'
                                            f'bb_pct,'
                                            f'fip,'
                                            f'csw_pct,'
                                            f'hard_pct,'
                                            f'groundball_pct,'
                                            f'swinging_strike_pct,'
                                            f'x_era,'
                                            f'x_woba,'
                                            f'null as pitcher_rank  '
                                f'from mv_pitcher_career_stats_by_position '
                                f'inner join players on mv_pitcher_career_stats_by_position.player_id = players.player_id '
                                f'where players.mlb_player_id = %s '
                                f'and mv_pitcher_career_stats_by_position."position" = \'ALL\' '
                            f') f '
                        f') g '
                        f'inner join lateral (select * from mv_starting_pitcher_averages where mv_starting_pitcher_averages.year_played = g.year_played) yearly_averages on true '
                        f'where g.qualified_rank is null ')
        
        def startingpitcherpitchpoolrankingslookup():
            return (f'select 	f.year_played,'
                                f'f.pitchtype,'
                                f'f.pitch_count,'
                                f'mv_starting_pitcher_pitch_pool.pitcher_rank as qualified_rank '
                        f'from '
                        f'( '
                            f'select 	year_played,'
                                    f'pitchtype,'
                                    f'pitcher_id,'
                                    f'sum(mv_pitcher_repertoire_by_position.num_pitches) as pitch_count '
                            f'from mv_pitcher_repertoire_by_position '
                            f'inner join players on players.player_id = mv_pitcher_repertoire_by_position.pitcher_id '
                            f'where mv_pitcher_repertoire_by_position.pitchtype != \'ALL\' '
                            f'and mv_pitcher_repertoire_by_position."position" = \'SP\' '
                            f'and players.mlb_player_id = %s '
                            f'group by year_played, pitchtype, pitcher_id '
                        f') f '
                        f'left join mv_starting_pitcher_pitch_pool on mv_starting_pitcher_pitch_pool.year_played = f.year_played and mv_starting_pitcher_pitch_pool.pitchtype = f.pitchtype and mv_starting_pitcher_pitch_pool.pitcher_id = f.pitcher_id '
                        )  

        def startingpitcherpitchcustomrankings():
            return (f'select	g.year_played as "year",'
                                f'g.pitchtype,'
                                f'g.qualified_rank AS "qualified-rank",'
                                f'g.usage_pct,'
                                f'g.usage_pct_percentile,'
                                f'g.usage_pct_rank,'
                                f'g.avg_velocity,'
                                f'g.avg_velocity_percentile,'
                                f'g.avg_velocity_rank,'
                                f'g.swinging_strike_pct,'
                                f'g.swinging_strike_pct_percentile,'
                                f'g.swinging_strike_pct_rank,'
                                f'g.csw_pct,'
                                f'g.csw_pct_percentile,'
                                f'g.csw_pct_rank,'
                                f'g.x_avg,'
                                f'g.x_avg_percentile,'
                                f'g.x_avg_rank,'
                                f'g.x_wobacon,'
                                f'g.x_wobacon_percentile,'
                                f'g.x_wobacon_rank,'
                                f'g.avg_x_movement,'
                                f'g.avg_x_movement_percentile,'
                                f'g.avg_x_movement_rank,'
                                f'g.avg_y_movement,'
                                f'g.avg_y_movement_percentile,'
                                f'g.avg_y_movement_rank,'
                                f'g.avg_spin_rate,'
                                f'g.avg_spin_rate_percentile,'
                                f'g.avg_spin_rate_rank,'
                                f'yearly_averages.usage_pct as league_usage_pct,'
                                f'yearly_averages.avg_velocity as league_avg_velocity,'
                                f'yearly_averages.swinging_strike_pct as league_swinging_strike_pct,'
                                f'yearly_averages.csw_pct as league_csw_pct,'
                                f'yearly_averages.x_avg as league_x_avg,'
                                f'yearly_averages.x_wobacon as league_x_wobacon,'
                                f'yearly_averages.avg_x_movement as league_avg_x_movement,'
                                f'yearly_averages.avg_y_movement as league_avg_y_movement,'
                                f'yearly_averages.avg_spin_rate as league_avg_spin_rate,'
                                f'yearly_averages.league_usage_pct_percentage,'
                                f'yearly_averages.league_avg_velocity_percentage,'
                                f'yearly_averages.league_swinging_strike_pct_percentage,'
                                f'yearly_averages.league_csw_pct_percentage,'
                                f'yearly_averages.league_x_avg_percentage,'
                                f'yearly_averages.league_x_wobacon_percentage,'
                                f'yearly_averages.league_avg_x_movement_percentage,'
                                f'yearly_averages.league_avg_y_movement_percentage,'
                                f'yearly_averages.league_avg_spin_rate_percentage '
                        f'from '
                        f'( '
                            f'select 	f.year_played,'
                                    f'f.pitchtype,'
                                    f'f.pitcher_rank AS qualified_rank,'
                                    f'f.usage_pct,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.usage_pct))::numeric, 2) AS usage_pct_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.usage_pct DESC) AS usage_pct_rank,'
                                    f'f.avg_velocity,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_velocity))::numeric, 2) AS avg_velocity_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_velocity DESC) AS avg_velocity_rank,'
                                    f'f.swinging_strike_pct,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.swinging_strike_pct))::numeric, 2) AS swinging_strike_pct_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.swinging_strike_pct DESC) AS swinging_strike_pct_rank,'
                                    f'f.csw_pct,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.csw_pct))::numeric, 2) AS csw_pct_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.csw_pct DESC) AS csw_pct_rank,'
                                    f'f.x_avg,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.x_avg DESC))::numeric, 2) AS x_avg_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.x_avg) AS x_avg_rank,'
                                    f'f.x_wobacon,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.x_wobacon DESC))::numeric, 2) AS x_wobacon_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.x_wobacon) AS x_wobacon_rank,'
                                    f'f.avg_x_movement,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_x_movement))::numeric, 2) AS avg_x_movement_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_x_movement DESC) AS avg_x_movement_rank,'
                                    f'f.avg_y_movement,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_y_movement))::numeric, 2) AS avg_y_movement_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_y_movement DESC) AS avg_y_movement_rank,'
                                    f'f.avg_spin_rate,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_spin_rate))::numeric, 2) AS avg_spin_rate_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_spin_rate DESC) AS avg_spin_rate_rank '
                            f'from '
                            f'( '
                                f'select 	year_played,'
                                        f'pitchtype,'
                                        f'usage_pct,'
                                        f'avg_velocity,'
                                        f'swinging_strike_pct,'
                                        f'csw_pct,'
                                        f'x_avg,'
                                        f'x_wobacon,'
                                        f'avg_x_movement,'
                                        f'avg_y_movement,'
                                        f'avg_spin_rate,'
                                        f'pitcher_rank '
                                f'from mv_starting_pitcher_pitch_pool '
                                f'inner join players on mv_starting_pitcher_pitch_pool.pitcher_id = players.player_id '
                                f'where mv_starting_pitcher_pitch_pool.pitchtype != \'ALL\' '
                                f'and players.mlb_player_id != %s  '
                                f'union '
                                f'select 	year_played,'
                                        f'pitchtype,'
                                        f'usage_pct,'
                                        f'avg_velocity,'
                                        f'swinging_strike_pct,'
                                        f'csw_pct,'
                                        f'x_avg,'
                                        f'x_wobacon,'
                                        f'avg_x_movement,'
                                        f'avg_y_movement,'
                                        f'avg_spin_rate,'
                                        f'0 as pitcher_rank '
                                f'from mv_pitcher_repertoire_by_position '
                                f'inner join players on mv_pitcher_repertoire_by_position.pitcher_id = players.player_id '
                                f'where players.mlb_player_id = %s '
                                f'and mv_pitcher_repertoire_by_position.pitchtype != \'ALL\' '
                                f'and mv_pitcher_repertoire_by_position.position = \'ALL\' '
                            f') f '
                        f') g '
                        f'inner join lateral (select * from mv_starting_pitcher_pitch_averages where mv_starting_pitcher_pitch_averages.year_played = g.year_played and mv_starting_pitcher_pitch_averages.pitchtype = g.pitchtype) yearly_averages on true '
                        f'where g.qualified_rank = 0 ')

        def reliefpitcherpoolrankingslookup():
            return (f'select 	mv_pitcher_career_stats."year"::int as "year",'
                               f' mv_relief_pitcher_pool.pitcher_rank as qualified_rank '
                        f'from players '
                        f'inner join mv_pitcher_career_stats on mv_pitcher_career_stats.pitcher_id = players.player_id '
                        f'left join mv_relief_pitcher_pool on mv_relief_pitcher_pool.year_played = mv_pitcher_career_stats."year"::int and mv_relief_pitcher_pool.player_id = mv_pitcher_career_stats.pitcher_id '
                        f'where mv_pitcher_career_stats."year" != \'ALL\' '
                        f'and mv_pitcher_career_stats.g > mv_pitcher_career_stats.gs '
                        f'and players.mlb_player_id = %s ')

        def reliefpitchercustomrankings():
            return (f'select 	g.year_played as "year",'
                                f'g.qualified_rank as "qualified-rank",'
                                f'g.w as "w",'
                                f'g.w_percentile as "w-percentile",'
                                f'g.w_rank as "w-rank",'
                                f'g.ip as "ip",'
                                f'g.ip_percentile as "ip-percentile",'
                                f'g.ip_rank as "ip-rank",'
                                f'g.era as "era",'
                                f'g.era_percentile as "era-percentile",'
                                f'g.era_rank as "era-rank",'
                                f'g.whip as "whip",'
                                f'g.whip_percentile as "whip-percentile",'
                                f'g.whip_rank as "whip-rank",'
                                f'g.k_pct as "k-pct",'
                                f'g.k_pct_percentile as "k-pct-percentile",'
                                f'g.k_pct_rank as "k-pct-rank",'
                                f'g.bb_pct as "bb-pct",'
                                f'g.bb_pct_percentile as "bb-pct-percentile",'
                                f'g.bb_pct_rank as "bb-pct-rank",'
                                f'g.csw_pct as "csw-pct",'
                                f'g.csw_pct_percentile as "csw-pct-percentile",'
                                f'g.csw_pct_rank as "csw-pct-rank",'
                                f'g.swinging_strike_pct as "swinging-strike-pct",'
                                f'g.swinging_strike_pct_percentile as "swinging-strike-pct-percentile",'
                                f'g.swinging_strike_pct_rank as "swinging-strike-pct-rank",'
                                f'g.hard_pct as "hard-pct",'
                                f'g.hard_pct_percentile as "hard-pct-percentile",'
                                f'g.hard_pct_rank as "hard-pct-rank",'
                                f'g.groundball_pct as "groundball-pct",'
                                f'g.groundball_pct_percentile as "groundball-pct-percentile",'
                                f'g.groundball_pct_rank as "groundball-pct-rank",'
                                f'g.x_era as "x-era",'
                                f'g.x_era_percentile as "x-era-percentile",'
                                f'g.x_era_rank as "x-era-rank",'
                                f'g.x_woba as "x-woba",'
                                f'g.x_woba_percentile as "x-woba-percentile",'
                                f'g.x_woba_rank as "x-woba-rank",'
                                f'yearly_averages.w as "league-w",'
                                f'yearly_averages.ip as "league-ip",'
                                f'yearly_averages.era as "league-era",'
                                f'yearly_averages.whip as "league-whip",'
                                f'yearly_averages.k_pct as "league-k-pct",'
                                f'yearly_averages.bb_pct as "league-bb-pct",'
                                f'yearly_averages.csw_pct as "league-csw-pct",'
                                f'yearly_averages.swinging_strike_pct as "league-swinging-strike-pct",'
                                f'yearly_averages.hard_pct as "league-hard-pct",'
                                f'yearly_averages.groundball_pct as "league-groundball-pct",'
                                f'yearly_averages.x_era as "league-x-era",'
                                f'yearly_averages.x_woba as "league-x-woba",'
                                f'yearly_averages.league_w_percentage as "league-w-percentile",'
                                f'yearly_averages.league_ip_percentage as "league-ip-percentile",'
                                f'yearly_averages.league_era_percentage as "league-era-percentile",'
                                f'yearly_averages.league_whip_percentage as "league-whip-percentile",'
                                f'yearly_averages.league_k_pct_percentage as "league-k-pct-percentile",'
                                f'yearly_averages.league_bb_pct_percentage as "league-bb-pct-percentile",'
                                f'yearly_averages.league_csw_pct_percentage as "league-csw-pct-percentile",'
                                f'yearly_averages.league_swinging_strike_pct_percentage as "league-swinging-strike-pct-percentile",'
                                f'yearly_averages.league_hard_pct_percentage as "league-hard-pct-percentile",'
                                f'yearly_averages.league_groundball_pct_percentage as "league-groundball-pct-percentile",'
                                f'yearly_averages.league_x_era_percentage as "league-x-era-percentile",'
                                f'yearly_averages.league_x_woba_percentage as "league-x-woba-percentile",'
                                f'g.sv as "sv",'
                                f'g.sv_percentile as "sv-percentile",'
                                f'g.sv_rank as "sv-rank",'
                                f'yearly_averages.sv as "league-sv",'
                                f'yearly_averages.league_sv_percentage as "league-sv-percentile" '
                        f'from '
                        f'( '
                            f'SELECT f.year_played,'
                                f'f.pitcher_rank AS qualified_rank,'
                                f'f.w,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.w) AS w_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.w DESC) AS w_rank,'
                                f'f.ip,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.ip) AS ip_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.ip DESC) AS ip_rank,'
                                f'f.era,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.era DESC) AS era_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.era) AS era_rank,'
                                f'f.whip,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.whip DESC) AS whip_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.whip) AS whip_rank,'
                                f'f.k_pct,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.k_pct) AS k_pct_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.k_pct DESC) AS k_pct_rank,'
                                f'f.bb_pct,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.bb_pct DESC) AS bb_pct_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.bb_pct) AS bb_pct_rank,'
                                f'f.csw_pct,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.csw_pct) AS csw_pct_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.csw_pct DESC) AS csw_pct_rank,'
                                f'f.swinging_strike_pct,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.swinging_strike_pct) AS swinging_strike_pct_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.swinging_strike_pct DESC) AS swinging_strike_pct_rank,'
                                f'f.hard_pct,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.hard_pct DESC) AS hard_pct_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.hard_pct) AS hard_pct_rank,'
                                f'f.groundball_pct,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.groundball_pct) AS groundball_pct_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.groundball_pct DESC) AS groundball_pct_rank,'
                                f'f.x_era,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.x_era DESC) AS x_era_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.x_era) AS x_era_rank,'
                                f'f.x_woba,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.x_woba DESC) AS x_woba_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.x_woba) AS x_woba_rank,'
                                f'f.sv,'
                                f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.sv) AS sv_percentile,'
                                f'rank() OVER (PARTITION BY f.year_played ORDER BY f.sv DESC) AS sv_rank '
                            f'from ' 
                            f' ('
                                f'select 	year_played, '
                                        f'"position",'
                                        f'g,'
                                        f'gs,'
                                        f'w,'
                                        f'l,'
                                        f'sv,'
                                        f'hld,'
                                        f'cg,'
                                        f'sho,'
                                        f'qs,'
                                        f'runs,'
                                        f'earned_runs,'
                                        f'hbp,'
                                        f'wp,'
                                        f'pa,'
                                        f'pitch_count,'
                                        f'pitches_per_game,'
                                        f'ip,'
                                        f'ip_per_game,'
                                        f'era,'
                                        f'whip,'
                                        f'lob_pct,'
                                        f'k_pct,'
                                        f'bb_pct,'
                                        f'fip,'
                                        f'csw_pct,'
                                        f'hard_pct,'
                                        f'groundball_pct,'
                                        f'swinging_strike_pct,'
                                        f'x_era,'
                                        f'x_woba,'
                                        f'pitcher_rank '
                                        f'from mv_relief_pitcher_pool '
                                        f'inner join players on mv_relief_pitcher_pool.player_id = players.player_id '
                                        f'where players.mlb_player_id != %s '
                                f'union '
                                f'select 	year_played, '
                                            f'"position",'
                                            f'g,'
                                            f'gs,'
                                            f'w,'
                                            f'l,'
                                            f'sv,'
                                            f'hld,'
                                            f'cg,'
                                            f'sho,'
                                            f'qs,'
                                            f'runs,'
                                            f'earned_runs,'
                                            f'hbp,'
                                            f'wp,'
                                            f'pa,'
                                            f'pitch_count,'
                                            f'pitches_per_game,'
                                            f'ip,'
                                            f'ip_per_game,'
                                            f'era,'
                                            f'whip,'
                                            f'lob_pct,'
                                            f'k_pct,'
                                            f'bb_pct,'
                                            f'fip,'
                                            f'csw_pct,'
                                            f'hard_pct,'
                                            f'groundball_pct,'
                                            f'swinging_strike_pct,'
                                            f'x_era,'
                                            f'x_woba,'
                                            f'null as pitcher_rank  '
                                f'from mv_pitcher_career_stats_by_position '
                                f'inner join players on mv_pitcher_career_stats_by_position.player_id = players.player_id '
                                f'where players.mlb_player_id = %s '
                                f'and mv_pitcher_career_stats_by_position."position" = \'ALL\' '
                            f') f '
                        f') g '
                        f'inner join lateral (select * from mv_relief_pitcher_averages where mv_relief_pitcher_averages.year_played = g.year_played) yearly_averages on true '
                        f'where g.qualified_rank is null ')
        
        def reliefpitcherpitchpoolrankingslookup():
            return (f'select 	f.year_played,'
                                f'f.pitchtype,'
                                f'f.pitch_count,'
                                f'mv_relief_pitcher_pitch_pool.pitcher_rank as qualified_rank '
                        f'from '
                        f'( '
                            f'select 	year_played,'
                                    f'pitchtype,'
                                    f'pitcher_id,'
                                    f'sum(mv_pitcher_repertoire_by_position.num_pitches) as pitch_count '
                            f'from mv_pitcher_repertoire_by_position '
                            f'inner join players on players.player_id = mv_pitcher_repertoire_by_position.pitcher_id '
                            f'where mv_pitcher_repertoire_by_position.pitchtype != \'ALL\' '
                            f'and mv_pitcher_repertoire_by_position."position" = \'RP\' '
                            f'and players.mlb_player_id = %s '
                            f'group by year_played, pitchtype, pitcher_id '
                        f') f '
                        f'left join mv_relief_pitcher_pitch_pool on mv_relief_pitcher_pitch_pool.year_played = f.year_played and mv_relief_pitcher_pitch_pool.pitchtype = f.pitchtype and mv_relief_pitcher_pitch_pool.pitcher_id = f.pitcher_id '
                        )
        
        def reliefpitcherpitchcustomrankings():
            return (f'select	g.year_played as "year",'
                                f'g.pitchtype,'
                                f'g.qualified_rank AS "qualified-rank",'
                                f'g.usage_pct,'
                                f'g.usage_pct_percentile,'
                                f'g.usage_pct_rank,'
                                f'g.avg_velocity,'
                                f'g.avg_velocity_percentile,'
                                f'g.avg_velocity_rank,'
                                f'g.swinging_strike_pct,'
                                f'g.swinging_strike_pct_percentile,'
                                f'g.swinging_strike_pct_rank,'
                                f'g.csw_pct,'
                                f'g.csw_pct_percentile,'
                                f'g.csw_pct_rank,'
                                f'g.x_avg,'
                                f'g.x_avg_percentile,'
                                f'g.x_avg_rank,'
                                f'g.x_wobacon,'
                                f'g.x_wobacon_percentile,'
                                f'g.x_wobacon_rank,'
                                f'g.avg_x_movement,'
                                f'g.avg_x_movement_percentile,'
                                f'g.avg_x_movement_rank,'
                                f'g.avg_y_movement,'
                                f'g.avg_y_movement_percentile,'
                                f'g.avg_y_movement_rank,'
                                f'g.avg_spin_rate,'
                                f'g.avg_spin_rate_percentile,'
                                f'g.avg_spin_rate_rank,'
                                f'yearly_averages.usage_pct as league_usage_pct,'
                                f'yearly_averages.avg_velocity as league_avg_velocity,'
                                f'yearly_averages.swinging_strike_pct as league_swinging_strike_pct,'
                                f'yearly_averages.csw_pct as league_csw_pct,'
                                f'yearly_averages.x_avg as league_x_avg,'
                                f'yearly_averages.x_wobacon as league_x_wobacon,'
                                f'yearly_averages.avg_x_movement as league_avg_x_movement,'
                                f'yearly_averages.avg_y_movement as league_avg_y_movement,'
                                f'yearly_averages.avg_spin_rate as league_avg_spin_rate,'
                                f'yearly_averages.league_usage_pct_percentage,'
                                f'yearly_averages.league_avg_velocity_percentage,'
                                f'yearly_averages.league_swinging_strike_pct_percentage,'
                                f'yearly_averages.league_csw_pct_percentage,'
                                f'yearly_averages.league_x_avg_percentage,'
                                f'yearly_averages.league_x_wobacon_percentage,'
                                f'yearly_averages.league_avg_x_movement_percentage,'
                                f'yearly_averages.league_avg_y_movement_percentage,'
                                f'yearly_averages.league_avg_spin_rate_percentage '
                        f'from '
                        f'( '
                            f'select 	f.year_played,'
                                    f'f.pitchtype,'
                                    f'f.pitcher_rank AS qualified_rank,'
                                    f'f.usage_pct,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.usage_pct))::numeric, 2) AS usage_pct_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.usage_pct DESC) AS usage_pct_rank,'
                                    f'f.avg_velocity,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_velocity))::numeric, 2) AS avg_velocity_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_velocity DESC) AS avg_velocity_rank,'
                                    f'f.swinging_strike_pct,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.swinging_strike_pct))::numeric, 2) AS swinging_strike_pct_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.swinging_strike_pct DESC) AS swinging_strike_pct_rank,'
                                    f'f.csw_pct,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.csw_pct))::numeric, 2) AS csw_pct_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.csw_pct DESC) AS csw_pct_rank,'
                                    f'f.x_avg,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.x_avg DESC))::numeric, 2) AS x_avg_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.x_avg) AS x_avg_rank,'
                                    f'f.x_wobacon,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.x_wobacon DESC))::numeric, 2) AS x_wobacon_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.x_wobacon) AS x_wobacon_rank,'
                                    f'f.avg_x_movement,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_x_movement))::numeric, 2) AS avg_x_movement_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_x_movement DESC) AS avg_x_movement_rank,'
                                    f'f.avg_y_movement,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_y_movement))::numeric, 2) AS avg_y_movement_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_y_movement DESC) AS avg_y_movement_rank,'
                                    f'f.avg_spin_rate,'
                                    f'round((100::numeric::double precision * percent_rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_spin_rate))::numeric, 2) AS avg_spin_rate_percentile,'
                                    f'rank() OVER (PARTITION BY f.year_played, f.pitchtype ORDER BY f.avg_spin_rate DESC) AS avg_spin_rate_rank '
                            f'from '
                            f'( '
                                f'select 	year_played,'
                                        f'pitchtype,'
                                        f'usage_pct,'
                                        f'avg_velocity,'
                                        f'swinging_strike_pct,'
                                        f'csw_pct,'
                                        f'x_avg,'
                                        f'x_wobacon,'
                                        f'avg_x_movement,'
                                        f'avg_y_movement,'
                                        f'avg_spin_rate,'
                                        f'pitcher_rank '
                                f'from mv_relief_pitcher_pitch_pool '
                                f'inner join players on mv_relief_pitcher_pitch_pool.pitcher_id = players.player_id '
                                f'where mv_relief_pitcher_pitch_pool.pitchtype != \'ALL\' '
                                f'and players.mlb_player_id != %s  '
                                f'union '
                                f'select 	year_played,'
                                        f'pitchtype,'
                                        f'usage_pct,'
                                        f'avg_velocity,'
                                        f'swinging_strike_pct,'
                                        f'csw_pct,'
                                        f'x_avg,'
                                        f'x_wobacon,'
                                        f'avg_x_movement,'
                                        f'avg_y_movement,'
                                        f'avg_spin_rate,'
                                        f'0 as pitcher_rank '
                                f'from mv_pitcher_repertoire_by_position '
                                f'inner join players on mv_pitcher_repertoire_by_position.pitcher_id = players.player_id '
                                f'where players.mlb_player_id = %s '
                                f'and mv_pitcher_repertoire_by_position.pitchtype != \'ALL\' '
                                f'and mv_pitcher_repertoire_by_position.position = \'ALL\' '
                            f') f '
                        f') g '
                        f'inner join lateral (select * from mv_relief_pitcher_pitch_averages where mv_relief_pitcher_pitch_averages.year_played = g.year_played and mv_relief_pitcher_pitch_averages.pitchtype = g.pitchtype) yearly_averages on true '
                        f'where g.qualified_rank = 0 ')

        def hitterpoolrankingslookup():
            return (f'select 	mv_hitter_career_stats."year"::int as "year",'
                               f' mv_hitter_pool.hitter_rank as qualified_rank '
                        f'from players '
                        f'inner join mv_hitter_career_stats on mv_hitter_career_stats.hitter_id = players.player_id '
                        f'left join mv_hitter_pool on mv_hitter_pool.year_played = mv_hitter_career_stats."year" and mv_hitter_pool.hitter_id = mv_hitter_career_stats.hitter_id '
                        f'where mv_hitter_career_stats."year" != \'ALL\' '
                        f'and mv_hitter_career_stats.g > 0 '
                        f'and players.mlb_player_id = %s ')

        def hittercustomrankings():
            return (f'select 	g.year_played::int as "year",'
                                f'g.qualified_rank as "qualified-rank",'
                                f'g.runs as "runs",'
                                f'g.runs_percentile as "runs-percentile",'
                                f'g.runs_rank as "runs-rank",'
                                f'g.hr as "hr",'
                                f'g.hr_percentile as "hr-percentile",'
                                f'g.hr_rank as "hr-rank",'
                                f'g.rbi as "rbi",'
                                f'g.rbi_percentile as "rbi-percentile",'
                                f'g.rbi_rank as "rbi-rank",'
                                f'g.sb as "sb",'
                                f'g.sb_percentile as "sb-percentile",'
                                f'g.sb_rank as "sb-rank",'
                                f'g.batting_average as "batting-average",'
                                f'g.batting_average_percentile as "batting-average-percentile",'
                                f'g.batting_average_rank as "batting-average-rank",'
                                f'g.on_base_pct as "on-base-pct",'
                                f'g.on_base_pct_percentile as "on-base-pct-percentile",'
                                f'g.on_base_pct_rank as "on-base-pct-rank",'
                                f'g.slug_pct as "slug-pct",'
                                f'g.slug_pct_percentile as "slug-pct-percentile",'
                                f'g.slug_pct_rank as "slug-pct-rank",'
                                f'g.k_pct as "k-pct",'
                                f'g.k_pct_percentile as "k-pct-percentile",'
                                f'g.k_pct_rank as "k-pct-rank",'
                                f'g.bb_pct as "bb-pct",'
                                f'g.bb_pct_percentile as "bb-pct-percentile",'
                                f'g.bb_pct_rank as "bb-pct-rank",'
                                f'g.ideal_pa_pct as "ideal-pa-pct",'
                                f'g.ideal_pa_pct_percentile as "ideal-pa-pct-percentile",'
                                f'g.ideal_pa_pct_rank as "ideal-pa-pct-rank",'
                                f'g.hard_pct as "hard-pct",'
                                f'g.hard_pct_percentile as "hard-pct-percentile",'
                                f'g.hard_pct_rank as "hard-pct-rank",'
                                f'g.x_avg as "x-avg",'
                                f'g.x_avg_percentile as "x-avg-percentile",'
                                f'g.x_avg_rank as "x-avg-rank",'
                                f'g.x_woba as "x-woba",'
                                f'g.x_woba_percentile as "x-woba-percentile",'
                                f'g.x_woba_rank as "x-woba-rank",'
                                f'yearly_averages.runs as "league-runs",'
                                f'yearly_averages.num_hr as "league-hr",'
                                f'yearly_averages.rbi as "league-rbi",'
                                f'yearly_averages.sb as "league-sb",'
                                f'yearly_averages.batting_average as "league-batting-average",'
                                f'yearly_averages.on_base_pct as "league-on-base-pct",'
                                f'yearly_averages.slug_pct as "league-slug-pct",'
                                f'yearly_averages.k_pct as "league-k-pct",'
                                f'yearly_averages.bb_pct as "league-bb-pct",'
                                f'yearly_averages.ideal_pa_pct as "league-ideal-pa-pct",'
                                f'yearly_averages.hard_pct as "league-hard-pct",'
                                f'yearly_averages.x_avg as "league-x-avg",'
                                f'yearly_averages.x_woba as "league-x-woba",'
                                f'yearly_averages.league_runs_percentage as "league-runs-percentile",'
                                f'yearly_averages.league_num_hr_percentage as "league-hr-percentile",'
                                f'yearly_averages.league_rbi_percentage as "league-rbi-percentile",'
                                f'yearly_averages.league_sb_percentage as "league-sb-percentile",'
                                f'yearly_averages.league_batting_average_percentage as "league-batting-average-percentile",'
                                f'yearly_averages.league_on_base_pct_percentage as "league-on-base-pct-percentile",'
                                f'yearly_averages.league_slug_pct_percentage as "league-slug-pct-percentile",'
                                f'yearly_averages.league_k_pct_percentage as "league-k-pct-percentile",'
                                f'yearly_averages.league_bb_pct_percentage as "league-bb-pct-percentile",'
                                f'yearly_averages.league_ideal_pa_pct_percentage as "league-ideal-pa-pct-percentile",'
                                f'yearly_averages.league_hard_pct_percentage as "league-hard-pct-percentile",'
                                f'yearly_averages.league_x_avg_percentage as "league-x-avg-percentile",'
                                f'yearly_averages.league_x_woba_percentage as "league-x-woba-percentile" '
                                f'from '
                                f'( '
                                    f'SELECT f.year_played,'
                                        f'f.hitter_rank AS qualified_rank,'
                                        f'f.runs,'
                                        f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.runs) AS runs_percentile,'
                                        f'rank() OVER (PARTITION BY f.year_played ORDER BY f.runs DESC) AS runs_rank,'
                                        f'f.num_hr as hr,'
                                        f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.num_hr) AS hr_percentile,'
                                        f'rank() OVER (PARTITION BY f.year_played ORDER BY f.num_hr DESC) AS hr_rank,'
                                        f'f.rbi,'
                                        f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.rbi) AS rbi_percentile,'
                                        f'rank() OVER (PARTITION BY f.year_played ORDER BY f.num_hr DESC) AS rbi_rank,'
                                        f'f.sb,'
                                        f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.sb) AS sb_percentile,'
                                        f'rank() OVER (PARTITION BY f.year_played ORDER BY f.sb DESC) AS sb_rank,'
                                        f'f.batting_average,'
                                        f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.batting_average) AS batting_average_percentile,'
                                        f'rank() OVER (PARTITION BY f.year_played ORDER BY f.batting_average DESC) AS batting_average_rank,'
                                        f'f.on_base_pct,'
                                        f' 100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.on_base_pct) AS on_base_pct_percentile,'
                                        f'rank() OVER (PARTITION BY f.year_played ORDER BY f.on_base_pct DESC) AS on_base_pct_rank,'
                                        f'f.slug_pct,'
                                        f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.slug_pct) AS slug_pct_percentile,'
                                        f'rank() OVER (PARTITION BY f.year_played ORDER BY f.slug_pct DESC) AS slug_pct_rank,'
                                        f'f.k_pct,'
                                        f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.k_pct desc) AS k_pct_percentile,'
                                        f'rank() OVER (PARTITION BY f.year_played ORDER BY f.k_pct) AS k_pct_rank,'
                                        f'f.bb_pct,'                                
                                        f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.bb_pct) AS bb_pct_percentile,'
                                        f'rank() OVER (PARTITION BY f.year_played ORDER BY f.bb_pct DESC) AS bb_pct_rank,'
                                        f'f.ideal_pa_pct,'
                                        f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.ideal_pa_pct) AS ideal_pa_pct_percentile,'
                                        f'rank() OVER (PARTITION BY f.year_played ORDER BY f.ideal_pa_pct DESC) AS ideal_pa_pct_rank,'
                                        f'f.hard_pct,'
                                        f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.hard_pct) AS hard_pct_percentile,'
                                        f'rank() OVER (PARTITION BY f.year_played ORDER BY f.hard_pct DESC) as hard_pct_rank,'
                                        f'f.x_avg,'
                                        f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.x_avg) AS x_avg_percentile,'
                                        f'rank() OVER (PARTITION BY f.year_played ORDER BY f.x_avg DESC) AS x_avg_rank,'
                                        f'f.x_woba,'
                                        f'100::double precision * percent_rank() OVER (PARTITION BY f.year_played ORDER BY f.x_woba) AS x_woba_percentile,'
                                        f'rank() OVER (PARTITION BY f.year_played ORDER BY f.x_woba DESC) AS x_woba_rank '
                                    f'from '
                                    f'( '
                                        f'select 	year_played,'
                                                f'runs,'
                                                f'num_hr,'
                                                f'rbi,'
                                                f'sb,'
                                                f'batting_average,'
                                                f'on_base_pct,'
                                                f'slug_pct,'
                                                f'k_pct,'
                                                f'bb_pct,'
                                                f'ideal_pa_pct,'
                                                f'hard_pct,'
                                                f'x_avg,'
                                                f'x_woba,'
                                                f'hitter_rank '
                                                f'from mv_hitter_pool '
                                                f'inner join players on mv_hitter_pool.hitter_id = players.player_id '
                                                f'where players.mlb_player_id != %s '
                                        f'union '
                                        f'select 	year_played,'
                                                f'runs,'
                                                f'num_hr,'
                                                f'rbi,'
                                                f'sb,'
                                                f'batting_average,'
                                                f'on_base_pct,'
                                                f'slug_pct,'
                                                f'k_pct,'
                                                f'bb_pct,'
                                                f'ideal_pa_pct,'
                                                f'hard_pct,'
                                                f'x_avg,'
                                                f'x_woba,'
                                                f'null as pitcher_rank  '
                                        f'from mv_hitter_page_stats '
                                        f'JOIN mv_hitter_career_stats ON mv_hitter_career_stats.hitter_id = mv_hitter_page_stats.hitter_id AND mv_hitter_page_stats.year_played = mv_hitter_career_stats.year '
                                        f'inner join players on mv_hitter_page_stats.hitter_id = players.player_id '
                                        f'WHERE mv_hitter_page_stats.pitchtype = \'ALL\'::text  '
                                        f'AND mv_hitter_page_stats.opponent_handedness = \'ALL\'::text '
                                        f'AND mv_hitter_page_stats.home_away = \'ALL\'::text '
                                        f'AND mv_hitter_page_stats.year_played <> \'ALL\'::text '
                                        f'and players.mlb_player_id = %s '
                                    f') f '
                                f') g '
                                f'inner join lateral (select * from mv_hitter_averages where mv_hitter_averages.year_played::text = g.year_played) yearly_averages on true '
                                f'where g.qualified_rank is null ')

        queries = {
            "abilities": abilities,
            "bio": bio,
            "career": career,
            "gamelogs": gamelogs,
            "locationlogs": locationlogs,
            "locations": locations,
            "positions": positions,
            "repertoire": stats,
            "stats": stats,
            "startingpitcherpoolrankingslookup": startingpitcherpoolrankingslookup,
            "startingpitchercustomrankings": startingpitchercustomrankings,
            "startingpitcherpitchpoolrankingslookup": startingpitcherpitchpoolrankingslookup,
            "startingpitcherpitchcustomrankings": startingpitcherpitchcustomrankings,
            "reliefpitcherpoolrankingslookup": reliefpitcherpoolrankingslookup,
            "reliefpitchercustomrankings": reliefpitchercustomrankings,
            "reliefpitcherpitchpoolrankingslookup": reliefpitcherpitchpoolrankingslookup,
            "reliefpitcherpitchcustomrankings": reliefpitcherpitchcustomrankings,
            "hitterpoolrankingslookup": hitterpoolrankingslookup,
            "hittercustomrankings": hittercustomrankings
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
            formatted_data = data.set_index(['gameid','pitchtype','split-RL'])
            return formatted_data

        def stats():
            if (self.is_pitcher):
                formatted_results = data.set_index(['pitchtype','year','split-RL','split-HA'])
            else:
                formatted_results = data.set_index(['pitchtype', 'year','split-RL','split-HA'])

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
            results['lastgame'] = pd.to_datetime(results['lastgame']).dt.strftime("%a %m/%d/%Y")
            results['birth_date'] = pd.to_datetime(results['birth_date']).dt.strftime("%a %m/%d/%Y")
            results['pro_debut_date'] = pd.to_datetime(results['pro_debut_date']).dt.strftime("%a %m/%d/%Y")
            results.fillna(value=json.dumps(None), inplace=True)

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

            # Convert datetime to usable json format
            results['game-date'] = pd.to_datetime(results['game-date']).dt.strftime("%a %m/%d/%Y")

            output_dict = { 'player_id': player_id, 'is_pitcher': self.is_pitcher, 'is_active': self.is_active, 'logs': {} }

            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            result_dict = json.loads(results.to_json(orient='index'))
            
            if (self.is_pitcher):

                # Drop cols that are not displayed on the front end
                # TODO: Add cols here that can safely be dropped as they are not used on the frontend.
                #results.drop(columns=['start','sac','csw'], inplace=True)

                for keys, value in result_dict.items():

                    # json coversion returns tuple string
                    key = eval(keys)
                    gameid_key = key[0]
                    if gameid_key not in output_dict['logs']:
                        output_dict['logs'][gameid_key] = { 'game': {
                            'year': value['year'],
                            'game-type': value['game-type'],
                            'gs': value['gs'], 
                            'g': value['g'],
                            'cg': value['cg'],
                            'w': value['w'], 
                            'l': value['l'], 
                            'sv': value['sv'],
                            'bsv': value['bsv'],
                            'hld': value['hld'],
                            'qs': value['qs'],
                            'ip': value['ip'],
                            'r': value['runs'],
                            'er': value['earned_runs'],
                            'pa': value['pa'],
                            'ab': value['ab'],
                            'lob': value['lob'],
                            'lob_pct': value['lob_pct'],
                            'park': value['park'],
                            'team-id': value['team-id'],
                            'team': value['team'],
                            'opponent-team-id': value['opponent-team-id'],
                            'opponent': value['opponent'],
                            'game-date': value['game-date'],
                            'team-result': value['team-result'],
                            'runs-scored': value['runs-scored'],
                            'opponent-runs-scored': value['opponent-runs-scored'],
                            'sho': value['sho'],
                            'era': value['era'],
                            'whip': value['whip'],
                            'x-era': value['x-era'],
                        }, 'pitches':{}}
                    
                    # Delete keys from value dict
                    del value['year']
                    del value['game-type']
                    del value['gs']
                    del value['g']
                    del value['cg']
                    del value['w']
                    del value['l']
                    del value['sv']
                    del value['bsv']
                    del value['hld']
                    del value['qs']
                    del value['ip']
                    del value['runs']
                    del value['earned_runs']
                    del value['lob']
                    del value['lob_pct']
                    del value['park']
                    del value['team-id']
                    del value['team']
                    del value['opponent-team-id']
                    del value['opponent']
                    del value['game-date']
                    del value['team-result']
                    del value['runs-scored']
                    del value['opponent-runs-scored']
                    del value['sho']
                    del value['era']
                    del value['whip']
                    del value['x-era']

                    pitch_key = key[1].upper()

                    if pitch_key not in output_dict['logs'][gameid_key]['pitches']:
                        output_dict['logs'][gameid_key]['pitches'][pitch_key] = {'splits':{}}
                    
                    rl_split_key = key[2].upper()
                    if rl_split_key not in output_dict['logs'][gameid_key]['pitches'][pitch_key]['splits']:
                        output_dict['logs'][gameid_key]['pitches'][pitch_key]['splits'][rl_split_key] = value
                
                return output_dict

            else:
                for keys, value in result_dict.items():

                    # json coversion returns tuple string
                    key = eval(keys)
                    gameid_key = key[0]
                    if gameid_key not in output_dict['logs']:
                        output_dict['logs'][gameid_key] = { 'game': {
                            'year': value['year'],
                            'game-type': value['game-type'],
                            'gs': value['gs'], 
                            'g': value['g'], 
                            'rbi':value['rbi'],
                            'r': value['runs'],
                            'sb': value['sb'],
                            'cs': value['cs'],
                            'park': value['park'],
                            'team-id': value['team-id'],
                            'team': value['team'],
                            'opponent-team-id': value['opponent-team-id'],
                            'opponent': value['opponent'],
                            'game-date': value['game-date'],
                            'team-result': value['team-result'],
                            'runs-scored': value['runs-scored'],
                            'opponent-runs-scored': value['opponent-runs-scored'],
                            'lob': value['lob'],
                            'batting-order-position': value['batting-order-position']
                        }, 'pitches':{}}
                    
                    # Delete keys from value dict
                    del value['year']
                    del value['game-type']
                    del value['gs']
                    del value['g']
                    del value['rbi']
                    del value['sb']
                    del value['cs']
                    del value['runs']
                    del value['park']
                    del value['team-id']
                    del value['team']
                    del value['opponent-team-id']
                    del value['opponent']
                    del value['game-date']
                    del value['team-result']
                    del value['runs-scored']
                    del value['opponent-runs-scored']
                    del value['batting-order-position']
                    del value['lob']
                    pitch_key = key[1].upper()

                    if pitch_key not in output_dict['logs'][gameid_key]['pitches']:
                        output_dict['logs'][gameid_key]['pitches'][pitch_key] = {'splits':{}}
                    
                    rl_split_key = key[2].upper()
                    if rl_split_key not in output_dict['logs'][gameid_key]['pitches'][pitch_key]['splits']:
                        output_dict['logs'][gameid_key]['pitches'][pitch_key]['splits'][rl_split_key] = value
                
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

                pitch_key = key[1].upper()

                if pitch_key not in output_dict['logs'][gameid_key]['pitches']:
                    output_dict['logs'][gameid_key]['pitches'][pitch_key] = {'splits':{}}
                
                rl_split_key = key[2].upper()
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
                    pitch_key = key[0].upper()

                    if pitch_key not in output_dict[query_type]['pitches']:
                        output_dict[query_type]['pitches'][pitch_key] = {'years':{}}

                    year_key = key[1]
                    stats = { 'total': self.career_stats[year_key], 'splits':{} } if (pitch_key == 'ALL') else { 'splits':{} }
                    if year_key not in output_dict[query_type]['pitches'][pitch_key]['years']:
                        output_dict[query_type]['pitches'][pitch_key]['years'][year_key] = stats
                    
                    rl_split_key = key[2].upper()
                    if rl_split_key not in output_dict[query_type]['pitches'][pitch_key]['years'][year_key]['splits']:
                        output_dict[query_type]['pitches'][pitch_key]['years'][year_key]['splits'][rl_split_key] = {'park':{}}
                
                    ha_split_key = key[3].upper() if (key[3] == 'All') else key[3]
                    output_dict[query_type]['pitches'][pitch_key]['years'][year_key]['splits'][rl_split_key]['park'][ha_split_key] = value       
            else:
                # Sort our DataFrame so we have a prettier JSON format for the API
                output_dict = { 'player_id': player_id, 'is_pitcher': self.is_pitcher, 'is_active': self.is_active, query_type: {'pitches':{}} }

                # Make sure our index keys exist in our dict structure then push on our data values
                for keys, value in result_dict.items():
                    # json coversion returns tuple string
                    key = eval(keys)
                    pitch_key = key[0].upper()

                    if pitch_key not in output_dict[query_type]['pitches']:
                        output_dict[query_type]['pitches'][pitch_key] = {'years':{}}

                    year_key = key[1]
                    stats = { 'total': self.career_stats[year_key], 'splits':{} } if (pitch_key == 'ALL') else { 'splits':{} }
                    if year_key not in output_dict[query_type]['pitches'][pitch_key]['years']:
                        output_dict[query_type]['pitches'][pitch_key]['years'][year_key] = stats
                    
                    rl_split_key = key[2].upper()
                    if rl_split_key not in output_dict[query_type]['pitches'][pitch_key]['years'][year_key]['splits']:
                        output_dict[query_type]['pitches'][pitch_key]['years'][year_key]['splits'][rl_split_key] = {'park':{}}
                
                    ha_split_key = key[3].upper() if (key[3] == 'All') else key[3]
                    output_dict[query_type]['pitches'][pitch_key]['years'][year_key]['splits'][rl_split_key]['park'][ha_split_key] = value     
                # Sort our DataFrame so we have a prettier JSON format for the API
                #output_dict = { 'player_id': player_id, 'is_pitcher': self.is_pitcher, 'is_active': self.is_active, query_type: {'years':{}} }

                # Make sure our index keys exist in our dict structure then push on our data values
                #for keys, value in result_dict.items():
                    # json coversion returns tuple string
                    #key = eval(keys)

                    #year_key = key[0]
                    #stats = { 'total': self.career_stats[year_key], 'splits':{} }
                    #if year_key not in output_dict[query_type]['years']:
                        #output_dict[query_type]['years'][year_key] = stats
                    
                    #rl_split_key = key[1].upper()
                    #if rl_split_key not in output_dict[query_type]['years'][year_key]['splits']:
                        #output_dict[query_type]['years'][year_key]['splits'][rl_split_key] = {'park':{}}
                
                    #ha_split_key = key[2]
                    #output_dict[query_type]['years'][year_key]['splits'][rl_split_key]['park'][ha_split_key] = value
            
            return output_dict

        json_data = {
            "bio": bio,
            "career": career,
            "gamelogs": gamelogs,
            "locationlogs": locationlogs,
            "locations": stats,
            "stats": stats,
            "repertoire": stats
        }

        return json_data.get(query_type, default)()
    
    def fetch_ranks_data(self, query_type, player_id):
        records = {}

        # Lookup if SP is ranked
        startingPitcherRankingsLookupQuery = self.get_query('startingpitcherpoolrankingslookup', player_id)
        startingPitcherRankingsLookup = fetch_dataframe(startingPitcherRankingsLookupQuery,player_id)
        
        # Only do SP calculations if there is SP data
        if(not startingPitcherRankingsLookup.empty):
            # Starting Pitcher Rankings Data
            startingPitcherRankingsQuery = self.get_query('startingpitchercustomrankings', player_id)
            startingPitcherRankings = fetch_dataframe(startingPitcherRankingsQuery, [player_id, player_id])

            # Lookup if SP pitch data is ranked
            startingPitcherPitchRankingsLookupQuery = self.get_query('startingpitcherpitchpoolrankingslookup', player_id)
            startingPitcherPitchRankingsLookup = fetch_dataframe(startingPitcherPitchRankingsLookupQuery,player_id)

            # Starting Pitcher Pitch Rankings Data
            startingPitcherPitchRankingsQuery = self.get_query('startingpitcherpitchcustomrankings', player_id)
            startingPitcherPitchRankings = fetch_dataframe(startingPitcherPitchRankingsQuery, [player_id, player_id])

            # Build SP and SP pitch dataframes
            (sp_rankings_df, sp_pitch_rankings_df) = self.build_pitcher_rank_dataframe(startingPitcherRankingsLookup, startingPitcherRankings, startingPitcherPitchRankingsLookup, startingPitcherPitchRankings)

            # Create a row for each year
            spYearGroupings = sp_rankings_df.groupby('year', sort=False)
            for year, yearValues in spYearGroupings:
                year_data = yearValues.iloc[0]
                sp_year_model = self.build_pitcher_year_model(year, year_data, sp_pitch_rankings_df)
                
                if not str(year) in records:    
                    records[str(year)] = {}
                records[str(year)]['SP'] = sp_year_model

        # Lookup if RP is ranked
        reliefPitcherRankingsLookupQuery = self.get_query('reliefpitcherpoolrankingslookup', player_id)
        reliefPitcherRankingsLookup = fetch_dataframe(reliefPitcherRankingsLookupQuery,player_id)

        # Only do RP calculations if there is RP data
        if(not reliefPitcherRankingsLookup.empty):
            # Relief Pitcher Rankings Data
            reliefPitcherRankingsQuery = self.get_query('reliefpitchercustomrankings', player_id)
            reliefPitcherRankings = fetch_dataframe(reliefPitcherRankingsQuery, [player_id, player_id])

            # Lookup if RP pitch data is ranked
            reliefPitcherPitchRankingsLookupQuery = self.get_query('reliefpitcherpitchpoolrankingslookup', player_id)
            reliefPitcherPitchRankingsLookup = fetch_dataframe(reliefPitcherPitchRankingsLookupQuery,player_id)

            # Relief Pitcher Pitch Rankings Data
            reliefPitcherPitchRankingsQuery = self.get_query('reliefpitcherpitchcustomrankings', player_id)
            reliefPitcherPitchRankings = fetch_dataframe(reliefPitcherPitchRankingsQuery, [player_id, player_id])

            # Build RP and RP pitch dataframes
            (rp_rankings_df, rp_pitch_rankings_df) = self.build_pitcher_rank_dataframe(reliefPitcherRankingsLookup, reliefPitcherRankings, reliefPitcherPitchRankingsLookup, reliefPitcherPitchRankings)

            # Create a row for each year
            rpYearGroupings = rp_rankings_df.groupby('year', sort=False)
            for year, yearValues in rpYearGroupings:
                year_data = yearValues.iloc[0]
                rp_year_model = self.build_pitcher_year_model(year, year_data, rp_pitch_rankings_df)
                
                if not str(year) in records:    
                    records[str(year)] = {}
                records[str(year)]['RP'] = rp_year_model

        # Lookup if hitter is ranked
        hitterRankingsLookupQuery = self.get_query('hitterpoolrankingslookup', player_id)
        hitterRankingsLookup = fetch_dataframe(hitterRankingsLookupQuery,player_id)
        
        # Only do RP calculations if there is RP data
        if(not hitterRankingsLookup.empty):
            # Hitter Pitcher Rankings Data
            hitterRankingsQuery = self.get_query('hittercustomrankings', player_id)
            hitterRankings = fetch_dataframe(hitterRankingsQuery, [player_id, player_id])

            hitter_rankings_df = self.build_hitter_rank_dataframe(hitterRankingsLookup, hitterRankings)

            # Create a row for each year
            hitterYearGroupings = hitter_rankings_df.groupby('year', sort=False)
            for year, yearValues in hitterYearGroupings:
                year_data = yearValues.iloc[0]
                hitter_year_model = self.build_hitter_year_model(year_data)
                
                if not str(year) in records:    
                    records[str(year)] = {}
                records[str(year)]['H'] = hitter_year_model

        return json.loads(json.dumps(records))

    def build_pitcher_rank_dataframe(self, seasonRankingLookupData, seasonRankingData, pitchRankingLookupData, pitchRankingData):
        rankings_df = pd.DataFrame()
        pitch_rankings_df = pd.DataFrame()
        for index, row in seasonRankingLookupData.iterrows():
            year = row['year']
            
            # Yearly Total Rankings
            year_rankings = {}

            qualified_rank = row['qualified_rank']
            matchingdata = seasonRankingData[seasonRankingData.year == year]
            # If there are zero or multiple matching records, do not attempt
            if len(matchingdata) != 1:
                continue
            year_rankings = matchingdata.head(1)

            year_rankings['is_qualified'] = not pd.isnull(qualified_rank) and not pd.isna(qualified_rank)

            rankings_df = rankings_df.append(year_rankings,ignore_index=True)

            # Individual Pitch Rankings
            yearpitches = pitchRankingLookupData[pitchRankingLookupData.year_played == year]
            for pitchindex, pitchrow in yearpitches.iterrows():
                year_pitch_rankings = {}
                pitchtype = pitchrow['pitchtype']
                qualified_pitch_rank = pitchrow['qualified_rank']
                isqualified = not pd.isnull(qualified_pitch_rank) and not pd.isna(qualified_pitch_rank)

                matchingdata = pitchRankingData[(pitchRankingData.year == year) & (pitchRankingData.pitchtype == pitchtype)]
                # If there are zero or multiple matching records, do not attempt
                if len(matchingdata) != 1:
                    continue
                year_pitch_rankings = matchingdata.head(1)
                year_pitch_rankings['is_qualified'] = bool(isqualified)

                pitch_rankings_df = pitch_rankings_df.append(year_pitch_rankings,ignore_index=True)
                

        rankings_df.fillna(value=0, inplace=True)            
        pitch_rankings_df.fillna(value=0, inplace=True)

        return rankings_df, pitch_rankings_df

    # Build rank model for this year and position
    def build_pitcher_year_model(self, year, year_data, pitch_data):
        year_model = {}

        year_model = {}
        year_model['is-qualified'] = bool(year_data['is_qualified'])

        w_model = {}
        w_model['player-stat-value'] = int(year_data['w'])
        w_model['player-stat-rank'] = int(year_data['w-rank'])
        w_model['player-stat-percentile'] = float(year_data['w-percentile'])
        w_model['league-average-stat-value'] = int(year_data['league-w'])
        w_model['league-average-stat-percentile'] = float(year_data['league-w-percentile'])
        year_model['w'] = w_model

        ip_model = {}
        ip_model['player-stat-value'] = float(year_data['ip'])
        ip_model['player-stat-rank'] = int(year_data['ip-rank'])
        ip_model['player-stat-percentile'] = float(year_data['ip-percentile'])
        ip_model['league-average-stat-value'] = int(year_data['league-ip'])
        ip_model['league-average-stat-percentile'] = float(year_data['league-ip-percentile'])
        year_model['ip'] = ip_model

        era_model = {}
        era_model['player-stat-value'] = float(year_data['era'])
        era_model['player-stat-rank'] = int(year_data['era-rank'])
        era_model['player-stat-percentile'] = float(year_data['era-percentile'])
        era_model['league-average-stat-value'] = float(year_data['league-era'])
        era_model['league-average-stat-percentile'] = float(year_data['league-era-percentile'])
        year_model['era'] = era_model

        whip_model = {}
        whip_model['player-stat-value'] = float(year_data['whip'])
        whip_model['player-stat-rank'] = int(year_data['whip-rank'])
        whip_model['player-stat-percentile'] = float(year_data['whip-percentile'])
        whip_model['league-average-stat-value'] = float(year_data['league-whip'])
        whip_model['league-average-stat-percentile'] = float(year_data['league-whip-percentile'])
        year_model['whip'] = whip_model

        k_pct_model = {}
        k_pct_model['player-stat-value'] = float(year_data['k-pct'])
        k_pct_model['player-stat-rank'] = int(year_data['k-pct-rank'])
        k_pct_model['player-stat-percentile'] = float(year_data['k-pct-percentile'])
        k_pct_model['league-average-stat-value'] = float(year_data['league-k-pct'])
        k_pct_model['league-average-stat-percentile'] = float(year_data['league-k-pct-percentile'])
        year_model['k_pct'] = k_pct_model

        bb_pct_model = {}
        bb_pct_model['player-stat-value'] = float(year_data['bb-pct'])
        bb_pct_model['player-stat-rank'] = int(year_data['bb-pct-rank'])
        bb_pct_model['player-stat-percentile'] = float(year_data['bb-pct-percentile'])
        bb_pct_model['league-average-stat-value'] = float(year_data['league-bb-pct'])
        bb_pct_model['league-average-stat-percentile'] = float(year_data['league-bb-pct-percentile'])
        year_model['bb_pct'] = bb_pct_model

        csw_pct_model = {}
        csw_pct_model['player-stat-value'] = float(year_data['csw-pct'])
        csw_pct_model['player-stat-rank'] = int(year_data['csw-pct-rank'])
        csw_pct_model['player-stat-percentile'] = float(year_data['csw-pct-percentile'])
        csw_pct_model['league-average-stat-value'] = float(year_data['league-csw-pct'])
        csw_pct_model['league-average-stat-percentile'] = float(year_data['league-csw-pct-percentile'])
        year_model['csw_pct'] = csw_pct_model

        swinging_strike_pct_model = {}
        swinging_strike_pct_model['player-stat-value'] = float(year_data['swinging-strike-pct'])
        swinging_strike_pct_model['player-stat-rank'] = int(year_data['swinging-strike-pct-rank'])
        swinging_strike_pct_model['player-stat-percentile'] = float(year_data['swinging-strike-pct-percentile'])
        swinging_strike_pct_model['league-average-stat-value'] = float(year_data['league-swinging-strike-pct'])
        swinging_strike_pct_model['league-average-stat-percentile'] = float(year_data['league-swinging-strike-pct-percentile'])
        year_model['swinging_strike_pct'] = swinging_strike_pct_model

        hard_pct_model = {}
        hard_pct_model['player-stat-value'] = float(year_data['hard-pct'])
        hard_pct_model['player-stat-rank'] = int(year_data['hard-pct-rank'])
        hard_pct_model['player-stat-percentile'] = float(year_data['hard-pct-percentile'])
        hard_pct_model['league-average-stat-value'] = float(year_data['league-hard-pct'])
        hard_pct_model['league-average-stat-percentile'] = float(year_data['league-hard-pct-percentile'])
        year_model['hard_pct'] = hard_pct_model

        groundball_pct_model = {}
        groundball_pct_model['player-stat-value'] = float(year_data['groundball-pct'])
        groundball_pct_model['player-stat-rank'] = int(year_data['groundball-pct-rank'])
        groundball_pct_model['player-stat-percentile'] = float(year_data['groundball-pct-percentile'])
        groundball_pct_model['league-average-stat-value'] = float(year_data['league-groundball-pct'])
        groundball_pct_model['league-average-stat-percentile'] = float(year_data['league-groundball-pct-percentile'])
        year_model['groundball_pct'] = groundball_pct_model

        x_era_model = {}
        x_era_model['player-stat-value'] = float(year_data['x-era'])
        x_era_model['player-stat-rank'] = int(year_data['x-era-rank'])
        x_era_model['player-stat-percentile'] = float(year_data['x-era-percentile'])
        x_era_model['league-average-stat-value'] = float(year_data['league-x-era'])
        x_era_model['league-average-stat-percentile'] = float(year_data['league-x-era-percentile'])
        year_model['x-era'] = x_era_model

        x_woba_model = {}
        x_woba_model['player-stat-value'] = float(year_data['x-woba'])
        x_woba_model['player-stat-rank'] = int(year_data['x-woba-rank'])
        x_woba_model['player-stat-percentile'] = float(year_data['x-woba-percentile'])
        x_woba_model['league-avwobage-stat-value'] = float(year_data['league-x-woba'])
        x_woba_model['league-avwobage-stat-percentile'] = float(year_data['league-x-woba-percentile'])
        year_model['x-woba-pct'] = x_woba_model

        if 'sv' in year_data:
            sv_model = {}
            sv_model['player-stat-value'] = int(year_data['sv'])
            sv_model['player-stat-rank'] = int(year_data['sv-rank'])
            sv_model['player-stat-percentile'] = float(year_data['sv-percentile'])
            sv_model['league-average-stat-value'] = int(year_data['league-sv'])
            sv_model['league-average-stat-percentile'] = float(year_data['league-sv-percentile'])
            year_model['sv'] = sv_model

        matching_year_pitches = pitch_data[pitch_data.year == year]
        pitches_model = {}
        # Create a record for each pitch
        for pitchindex, pitchrow in matching_year_pitches.iterrows():
            pitch_model = {}
            pitchtype = pitchrow['pitchtype']
            pitch_model['is-qualified'] = bool(pitchrow['is_qualified'])

            usage_pct_model = {}
            usage_pct_model['player-stat-value'] = float(pitchrow['usage_pct'])
            usage_pct_model['player-stat-rank'] = int(pitchrow['usage_pct_rank'])
            usage_pct_model['player-stat-percentile'] = float(pitchrow['usage_pct_percentile'])
            usage_pct_model['league-average-stat-value'] = float(pitchrow['league_usage_pct'])
            usage_pct_model['league-average-stat-percentile'] = float(pitchrow['league_usage_pct_percentage'])
            pitch_model['usage_pct'] = usage_pct_model

            avg_velocity_model = {}
            avg_velocity_model['player-stat-value'] = float(pitchrow['avg_velocity'])
            avg_velocity_model['player-stat-rank'] = int(pitchrow['avg_velocity_rank'])
            avg_velocity_model['player-stat-percentile'] = float(pitchrow['avg_velocity_percentile'])
            avg_velocity_model['league-average-stat-value'] = float(pitchrow['league_avg_velocity'])
            avg_velocity_model['league-average-stat-percentile'] = float(pitchrow['league_avg_velocity_percentage'])
            pitch_model['avg_velocity'] = avg_velocity_model

            swinging_strike_pct_model = {}
            swinging_strike_pct_model['player-stat-value'] = float(pitchrow['swinging_strike_pct'])
            swinging_strike_pct_model['player-stat-rank'] = int(pitchrow['swinging_strike_pct_rank'])
            swinging_strike_pct_model['player-stat-percentile'] = float(pitchrow['swinging_strike_pct_percentile'])
            swinging_strike_pct_model['league-average-stat-value'] = float(pitchrow['league_swinging_strike_pct'])
            swinging_strike_pct_model['league-average-stat-percentile'] = float(pitchrow['league_swinging_strike_pct_percentage'])
            pitch_model['swinging_strike_pct'] = swinging_strike_pct_model

            csw_pct_model = {}
            csw_pct_model['player-stat-value'] = float(pitchrow['csw_pct'])
            csw_pct_model['player-stat-rank'] = int(pitchrow['csw_pct_rank'])
            csw_pct_model['player-stat-percentile'] = float(pitchrow['csw_pct_percentile'])
            csw_pct_model['league-average-stat-value'] = float(pitchrow['league_csw_pct'])
            csw_pct_model['league-average-stat-percentile'] = float(pitchrow['league_csw_pct_percentage'])
            pitch_model['csw_pct'] = csw_pct_model

            x_avg_model = {}
            x_avg_model['player-stat-value'] = float(pitchrow['x_avg'])
            x_avg_model['player-stat-rank'] = int(pitchrow['x_avg_rank'])
            x_avg_model['player-stat-percentile'] = float(pitchrow['x_avg_percentile'])
            x_avg_model['league-average-stat-value'] = float(pitchrow['league_x_avg'])
            x_avg_model['league-average-stat-percentile'] = float(pitchrow['league_x_avg_percentage'])
            pitch_model['x-avg'] = x_avg_model

            x_wobacon_model = {}
            x_wobacon_model['player-stat-value'] = float(pitchrow['x_wobacon'])
            x_wobacon_model['player-stat-rank'] = int(pitchrow['x_wobacon_rank'])
            x_wobacon_model['player-stat-percentile'] = float(pitchrow['x_wobacon_percentile'])
            x_wobacon_model['league-average-stat-value'] = float(pitchrow['league_x_wobacon'])
            x_wobacon_model['league-average-stat-percentile'] = float(pitchrow['league_x_wobacon_percentage'])
            pitch_model['x-wobacon-pct'] = x_wobacon_model

            avg_x_movement_model = {}
            avg_x_movement_model['player-stat-value'] = float(pitchrow['avg_x_movement'])
            avg_x_movement_model['player-stat-rank'] = int(pitchrow['avg_x_movement_rank'])
            avg_x_movement_model['player-stat-percentile'] = float(pitchrow['avg_x_movement_percentile'])
            avg_x_movement_model['league-average-stat-value'] = float(pitchrow['league_avg_x_movement'])
            avg_x_movement_model['league-average-stat-percentile'] = float(pitchrow['league_avg_x_movement_percentage'])
            pitch_model['x-release-avg'] = avg_x_movement_model

            avg_y_movement_model = {}
            avg_y_movement_model['player-stat-value'] = float(pitchrow['avg_y_movement'])
            avg_y_movement_model['player-stat-rank'] = int(pitchrow['avg_y_movement_rank'])
            avg_y_movement_model['player-stat-percentile'] = float(pitchrow['avg_y_movement_percentile'])
            avg_y_movement_model['league-average-stat-value'] = float(pitchrow['league_avg_y_movement'])
            avg_y_movement_model['league-average-stat-percentile'] = float(pitchrow['league_avg_y_movement_percentage'])
            pitch_model['y-release-avg'] = avg_y_movement_model

            avg_spin_rate_model = {}
            avg_spin_rate_model['player-stat-value'] = float(pitchrow['avg_spin_rate'])
            avg_spin_rate_model['player-stat-rank'] = int(pitchrow['avg_spin_rate_rank'])
            avg_spin_rate_model['player-stat-percentile'] = float(pitchrow['avg_spin_rate_percentile'])
            avg_spin_rate_model['league-average-stat-value'] = float(pitchrow['league_avg_spin_rate'])
            avg_spin_rate_model['league-average-stat-percentile'] = float(pitchrow['league_avg_spin_rate_percentage'])
            pitch_model['spin-rate-avg'] = avg_spin_rate_model


            pitches_model[pitchtype] = pitch_model

        year_model['pitches'] = pitches_model

        return year_model


    def build_hitter_rank_dataframe(self, seasonRankingLookupData, seasonRankingData):
        rankings_df = pd.DataFrame()
        for index, row in seasonRankingLookupData.iterrows():
            year = row['year']
            
            # Yearly Total Rankings
            year_rankings = {}

            qualified_rank = row['qualified_rank']
            matchingdata = seasonRankingData[seasonRankingData.year == year]
            # If there are zero or multiple matching records, do not attempt
            if len(matchingdata) != 1:
                continue
            year_rankings = matchingdata.head(1)

            year_rankings['is_qualified'] = not pd.isnull(qualified_rank) and not pd.isna(qualified_rank)

            rankings_df = rankings_df.append(year_rankings,ignore_index=True)                

        rankings_df.fillna(value=0, inplace=True)

        return rankings_df
    
        # Build rank model for this year and position
    def build_hitter_year_model(self, year_data):
        year_model = {}

        year_model = {}
        year_model['is-qualified'] = bool(year_data['is_qualified'])

        runs_model = {}
        runs_model['player-stat-value'] = int(year_data['runs'])
        runs_model['player-stat-rank'] = int(year_data['runs-rank'])
        runs_model['player-stat-percentile'] = float(year_data['runs-percentile'])
        runs_model['league-average-stat-value'] = int(year_data['league-runs'])
        runs_model['league-average-stat-percentile'] = float(year_data['league-runs-percentile'])
        year_model['runs'] = runs_model

        hr_model = {}
        hr_model['player-stat-value'] = float(year_data['hr'])
        hr_model['player-stat-rank'] = int(year_data['hr-rank'])
        hr_model['player-stat-percentile'] = float(year_data['hr-percentile'])
        hr_model['league-average-stat-value'] = int(year_data['league-hr'])
        hr_model['league-average-stat-percentile'] = float(year_data['league-hr-percentile'])
        year_model['hr'] = hr_model

        rbi_model = {}
        rbi_model['player-stat-value'] = float(year_data['rbi'])
        rbi_model['player-stat-rank'] = int(year_data['rbi-rank'])
        rbi_model['player-stat-percentile'] = float(year_data['rbi-percentile'])
        rbi_model['league-average-stat-value'] = float(year_data['league-rbi'])
        rbi_model['league-average-stat-percentile'] = float(year_data['league-rbi-percentile'])
        year_model['rbi'] = rbi_model

        sb_model = {}
        sb_model['player-stat-value'] = float(year_data['sb'])
        sb_model['player-stat-rank'] = int(year_data['sb-rank'])
        sb_model['player-stat-percentile'] = float(year_data['sb-percentile'])
        sb_model['league-average-stat-value'] = float(year_data['league-sb'])
        sb_model['league-average-stat-percentile'] = float(year_data['league-sb-percentile'])
        year_model['sb'] = sb_model

        batting_average_model = {}
        batting_average_model['player-stat-value'] = float(year_data['batting-average'])
        batting_average_model['player-stat-rank'] = int(year_data['batting-average-rank'])
        batting_average_model['player-stat-percentile'] = float(year_data['batting-average-percentile'])
        batting_average_model['league-average-stat-value'] = float(year_data['league-batting-average'])
        batting_average_model['league-average-stat-percentile'] = float(year_data['league-batting-average-percentile'])
        year_model['batting_avg'] = batting_average_model

        obp_model = {}
        obp_model['player-stat-value'] = float(year_data['on-base-pct'])
        obp_model['player-stat-rank'] = int(year_data['on-base-pct-rank'])
        obp_model['player-stat-percentile'] = float(year_data['on-base-pct-percentile'])
        obp_model['league-average-stat-value'] = float(year_data['league-on-base-pct'])
        obp_model['league-average-stat-percentile'] = float(year_data['league-on-base-pct-percentile'])
        year_model['on-base-pct'] = obp_model

        slug_model = {}
        slug_model['player-stat-value'] = float(year_data['slug-pct'])
        slug_model['player-stat-rank'] = int(year_data['slug-pct-rank'])
        slug_model['player-stat-percentile'] = float(year_data['slug-pct-percentile'])
        slug_model['league-average-stat-value'] = float(year_data['league-slug-pct'])
        slug_model['league-average-stat-percentile'] = float(year_data['league-slug-pct-percentile'])
        year_model['slug-pct'] = slug_model

        k_pct_model = {}
        k_pct_model['player-stat-value'] = float(year_data['k-pct'])
        k_pct_model['player-stat-rank'] = int(year_data['k-pct-rank'])
        k_pct_model['player-stat-percentile'] = float(year_data['k-pct-percentile'])
        k_pct_model['league-average-stat-value'] = float(year_data['league-k-pct'])
        k_pct_model['league-average-stat-percentile'] = float(year_data['league-k-pct-percentile'])
        year_model['k_pct'] = k_pct_model

        bb_pct_model = {}
        bb_pct_model['player-stat-value'] = float(year_data['bb-pct'])
        bb_pct_model['player-stat-rank'] = int(year_data['bb-pct-rank'])
        bb_pct_model['player-stat-percentile'] = float(year_data['bb-pct-percentile'])
        bb_pct_model['league-average-stat-value'] = float(year_data['league-bb-pct'])
        bb_pct_model['league-average-stat-percentile'] = float(year_data['league-bb-pct-percentile'])
        year_model['bb_pct'] = bb_pct_model

        ideal_pa_pct_model = {}
        ideal_pa_pct_model['player-stat-value'] = float(year_data['ideal-pa-pct'])
        ideal_pa_pct_model['player-stat-rank'] = int(year_data['ideal-pa-pct-rank'])
        ideal_pa_pct_model['player-stat-percentile'] = float(year_data['ideal-pa-pct-percentile'])
        ideal_pa_pct_model['league-average-stat-value'] = float(year_data['league-ideal-pa-pct'])
        ideal_pa_pct_model['league-average-stat-percentile'] = float(year_data['league-ideal-pa-pct-percentile'])
        year_model['ideal-pa-pct'] = ideal_pa_pct_model

        hard_pct_model = {}
        hard_pct_model['player-stat-value'] = float(year_data['hard-pct'])
        hard_pct_model['player-stat-rank'] = int(year_data['hard-pct-rank'])
        hard_pct_model['player-stat-percentile'] = float(year_data['hard-pct-percentile'])
        hard_pct_model['league-average-stat-value'] = float(year_data['league-hard-pct'])
        hard_pct_model['league-average-stat-percentile'] = float(year_data['league-hard-pct-percentile'])
        year_model['hard_pct'] = hard_pct_model

        x_avg_model = {}
        x_avg_model['player-stat-value'] = float(year_data['x-avg'])
        x_avg_model['player-stat-rank'] = int(year_data['x-avg-rank'])
        x_avg_model['player-stat-percentile'] = float(year_data['x-avg-percentile'])
        x_avg_model['league-average-stat-value'] = float(year_data['league-x-avg'])
        x_avg_model['league-average-stat-percentile'] = float(year_data['league-x-avg-percentile'])
        year_model['x-avg'] = x_avg_model

        x_woba_model = {}
        x_woba_model['player-stat-value'] = float(year_data['x-woba'])
        x_woba_model['player-stat-rank'] = int(year_data['x-woba-rank'])
        x_woba_model['player-stat-percentile'] = float(year_data['x-woba-percentile'])
        x_woba_model['league-avwobage-stat-value'] = float(year_data['league-x-woba'])
        x_woba_model['league-avwobage-stat-percentile'] = float(year_data['league-x-woba-percentile'])
        year_model['x-woba-pct'] = x_woba_model

        return year_model
   
