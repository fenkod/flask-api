from flask import current_app
from flask_restful import Resource
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
                                        f'CAST(null as date) as "lastgame",'
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
                                        f'highest_pitcher_depth_chart_position.depth as pitcher_depth_chart_depth '
                                f'from players '
                                f'left join teams as current_team on current_team.team_id = players.current_team_id '
                                f'left join teams as draft_team on draft_team.team_id = players.draft_team_id '
                                f'left join lateral (select * from depth_charts where depth_charts.player_id = players.player_id and depth_charts.team_id = current_team.team_id and depth_charts."position" not in (\'SP\', \'BP\', \'CL\') order by depth_charts."depth" fetch first row only) highest_hitter_depth_chart_position on true '
                                f'left join lateral (select * from depth_charts where depth_charts.player_id = players.player_id and depth_charts.team_id = current_team.team_id and depth_charts."position" in (\'SP\', \'BP\', \'CL\') order by depth_charts."depth" fetch first row only) highest_pitcher_depth_chart_position on true  \n')
            player_select = ''

            if player_id != 'NA':
                player_select = 'WHERE players.mlb_player_id = %s'

            sql_query = table_select + player_select

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
                        f'runs::int, '
                        f'rbi::int, '
                        f'sb::int, '
                        f'cs::int, '
                        f'teams '
                    f'FROM mv_hitter_career_stats '
                    f'WHERE hittermlbamid = %s '
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
                            f'mv_pitcher_game_logs.num_outs AS "outs",'
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
                            f'x_wobacon as "x-wobacon-pct" '
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
                        f'games.game_date AS "game-date",'
                        f'games.year_played as "year",'
                        f'games.game_type as "game-type",'
                        f'hitter_team.abbreviation as team,'
                        f'hitter_team.team_id::int AS "team-id",'
                        f'opponent_team.abbreviation as opponent,'
                        f'opponent_team.team_id::int AS "opponent-team-id",'
                        f'case when hitter_team.team_id = games.home_team_id then \'HOME\' else \'AWAY\' end as park,'
                        f'case when games.status = \'closed\' then (case when hitter_team.team_id = games.winning_team_id then \'Win\' else \'Loss\' end) else null end AS "team-result",'
                        f'(case when hitter_team.team_id = games.home_team_id then games.home_team_runs else games.away_team_runs end)::int AS "runs-scored",'
                        f'(case when hitter_team.team_id = games.home_team_id then games.away_team_runs else games.home_team_runs end)::int AS "opponent-runs-scored",'
                        f'hitting_game_log.runs::int AS "runs",'
                        f'hitting_game_log."start"::int  as "gs",'
                        f'hitting_game_log.rbi::int as "rbi",'
                        f'hitting_game_log.sb::int as "sb",'
                        f'hitting_game_log.cs::int as "cs",'
                        f'hitting_game_log.ap::int as "pa",'
                        f'hitting_game_log.ab::int as "ab",'
                        f'hitting_game_log.lob::int as "lob",'
                        f'\'FF\'::pitch_type as pitchtype,'
                        f'\'R\'::player_side AS "split-RL",'
                        f'null::numeric AS "velo_avg",'
                        f'null::numeric AS strikeout_pct,'
                        f'null::numeric AS bb_pct,'
                        f'null::numeric AS usage_pct,'
                        f'null::numeric AS "batting_avg",' 
                        f'null::numeric AS o_swing_pct,'
                        f'null::numeric as z_swing_pct,'
                        f'null::numeric AS zone_pct,'
                        f'null::numeric AS swinging_strike_pct,'
                        f'null::numeric AS called_strike_pct,'
                        f'null::numeric AS csw_pct,'
                        f'null::numeric AS cswf_pct,'
                        f'null::numeric AS plus_pct,'
                        f'null::numeric AS foul_pct,'
                        f'null::numeric AS contact_pct,'
                        f'null::numeric AS o_contact_pct,'
                        f'null::numeric AS z_contact_pct,'
                        f'null::numeric AS swing_pct,'
                        f'null::numeric AS strike_pct,'
                        f'null::numeric AS early_called_strike_pct,'
                        f'null::numeric AS late_o_swing_pct,'
                        f'null::numeric AS f_strike_pct,'
                        f'null::numeric AS true_f_strike_pct,'
                        f'null::numeric AS groundball_pct,'
                        f'null::numeric AS linedrive_pct,'
                        f'null::numeric AS flyball_pct,'
                        f'null::numeric AS hr_flyball_pct,'
                        f'null::numeric AS groundball_flyball_pct,'
                        f'null::numeric AS infield_flyball_pct,'
                        f'null::numeric AS weak_pct,'
                        f'null::numeric AS medium_pct,'
                        f'null::numeric AS hard_pct,'
                        f'null::numeric AS center_pct,'
                        f'null::numeric AS pull_pct,'
                        f'null::numeric AS opposite_field_pct,'
                        f'null::numeric AS babip_pct,'
                        f'null::numeric AS bacon_pct,'
                        f'null::numeric AS armside_pct,'
                        f'null::numeric AS gloveside_pct,'
                        f'null::numeric AS "v_mid_pct",'
                        f'null::numeric AS "h_mid_pct",'
                        f'null::numeric AS high_pct,'
                        f'null::numeric AS low_pct,'
                        f'null::numeric AS heart_pct,'
                        f'null::numeric AS early_pct,'
                        f'null::numeric AS behind_pct,'
                        f'null::numeric AS late_pct,'
                        f'null::numeric AS non_bip_strike_pct,'
                        f'null::numeric AS early_bip_pct,'
                        f'null::int AS "pitch-count",'
                        f'null::int AS "hits",'
                        f'null::int AS "bb",'
                        f'null::int AS "1b",' 
                        f'null::int AS "2b",' 
                        f'null::int AS "3b",' 
                        f'null::int AS "hr",' 
                        f'null::int AS "k",'
                        f'null::int AS "strikes",' 
                        f'null::int AS "balls",' 
                        f'null::int AS "foul",' 
                        f'null::int AS "ibb",' 
                        f'null::int AS "hbp",' 
                        f'null::int as "flyball",'
                        f'null::int as "whiff",'
                        f'null::int as "zone",'
                        f'null::int astotal_velo,'
                        f'null::int as "pitches-clocked",'
                        f'null::int as "armside",'
                        f'null::int as "gloveside",'
                        f'null::int as "inside",'
                        f'null::int as "outside",'
                        f'null::int as "h-mid",'
                        f'null::int as "high",'
                        f'null::int as "mid",'
                        f'null::int as "low",'
                        f'null::int as "heart",'
                        f'null::int as "early",'
                        f'null::int as "late",'
                        f'null::int as "behind",'
                        f'null::int as "non-bip-strike",'
                        f'null::int as "batted-ball-event",'
                        f'null::int as "early-bip",'
                        f'null::int as "fastball",'
                        f'null::int as "secondary",'
                        f'null::int as "early-secondary",'
                        f'null::int as "late-secondary",'
                        f'null::int as "called-strike",'
                        f'null::int as "early-called-strike",'
                        f'null::int as "putaway",'
                        f'null::int as "swings",'
                        f'null::int as "contact",'
                        f'null::int as "first-pitch-swings",'
                        f'null::int "first-pitch-strikes",'
                        f'null::int as "true-first-pitch-strikes",'
                        f'null::int as "plus-pitch",'
                        f'null::int as "z-swing",'
                        f'null::int as "z-contact",'
                        f'null::int as "o-swing",'
                        f'null::int as "o-contact",'
                        f'null::int as "early-o-swing",'
                        f'null::int as "early-o-contact",'
                        f'null::int as "late-o-swing",'
                        f'null::int as "late-o-contact",'
                        f'null::int as "pulled-bip",'
                        f'null::int as "opp-bip",'
                        f'null::int as "line-drive",'
                        f'null::int as "if-flyball",'
                        f'null::int as "groundball",'
                        f'null::int as "weak-bip",'
                        f'null::int "medium-bip",'
                        f'null::int as "hard-bip",'
                        f'null::numeric AS "ops" '
                    f'from hitting_game_log '
                    f'left join teams as hitter_team on hitter_team.team_id = hitting_game_log.team_id '
                    f'left join games on games.game_id = hitting_game_log.game_id '
                    f'left join teams as opponent_team on opponent_team.team_id = (case when games.home_team_id = hitting_game_log.team_id then games.away_team_id else games.home_team_id end) '
                    f'left join players on players.player_id = hitting_game_log.player_id '
                    f'where players.mlb_player_id = %s ' #and game_played >= current_date - interval \'400 day\'
                    f'order by game_date desc;'
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
                    f'SELECT DISTINCT ghuid AS "gameid",'
                        f'pitchtype,'
                        f'pitcherside AS "split-RL",'
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
                    f'FROM mv_hitter_game_log_pitches '
                    f"WHERE hittermlbamid = %s "
                    f'ORDER BY ghuid;'
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
                                f'x_wobacon as "x-wobacon-pct" '
                            f'FROM player_page_repertoire '
                            f'inner join players on players.player_id = player_page_repertoire.pitcher_id '
                            f'WHERE players.mlb_player_id = %s '
                            f'ORDER BY pitchtype, year_played, opponent_handedness, home_away;'
                )
            else:
                return (
                    f'SELECT year_played AS "year",' 
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
                        f'onbase_pct,'
                        f'ops_pct,'
                        f'null::int AS "wob_avg",'
                        f'early_o_contact_pct,'
                        f'late_o_contact_pct,'
                        f'first_pitch_swing_pct,'
                        f'num_pitches AS "pitch-count",'
                        f'num_hits AS "hits",' 
                        f'num_bb AS "bb",' 
                        f'num_1b AS "1b",' 
                        f'num_2b AS "2b",' 
                        f'num_3b AS "3b",' 
                        f'num_hr AS "hr",'
                        f'num_k AS "k",'
                        f'num_pa AS "pa",'
                        f'num_strike AS "strikes",' 
                        f'num_ball AS "balls",' 
                        f'null::int AS "foul",' 
                        f'null::int AS "ibb",' 
                        f'null::int AS "hbp",'
                        f'num_rbi AS "rbi" '
                    f'FROM mv_hitter_page_stats '
                    f"WHERE hittermlbamid = %s "
                    f'ORDER BY year_played, opponent_handedness, home_away; '
                )

        queries = {
            "abilities": abilities,
            "bio": bio,
            "career": career,
            "gamelogs": gamelogs,
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
                            'lob': value['lob']
                        }, 'pitches':{}}
                    
                    # Delete keys from value dict
                    del value['year']
                    del value['game-type']
                    del value['gs']
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
                output_dict = { 'player_id': player_id, 'is_pitcher': self.is_pitcher, 'is_active': self.is_active, query_type: {'years':{}} }

                # Make sure our index keys exist in our dict structure then push on our data values
                for keys, value in result_dict.items():
                    # json coversion returns tuple string
                    key = eval(keys)

                    year_key = key[0]
                    stats = { 'total': self.career_stats[year_key], 'splits':{} }
                    if year_key not in output_dict[query_type]['years']:
                        output_dict[query_type]['years'][year_key] = stats
                    
                    rl_split_key = key[1].upper()
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
            "stats": stats,
            "repertoire": stats
        }

        return json_data.get(query_type, default)()
