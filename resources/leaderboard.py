from signal import raise_signal
from flask import current_app, Flask, request
from flask_restful import Resource
import pandas as pd
from pandas import DataFrame
from errorhandler.errorhandler import InvalidUsage
from helpers import fetch_dataframe, get_team_info, weightedonbasepercentage, var_dump
from cache import cache_timeout, cache_invalidate_hour
from datetime import date, datetime
from webargs import fields, validate
from webargs.flaskparser import use_kwargs, parser, abort
import json as json
from marshmallow import Schema, fields

##
# This is the flask_restful Resource Class for the Leaderboard API.
##
class Leaderboard(Resource):
    teams = get_team_info()
    valid_teams = list(teams.keys())
    valid_years = ['2021']
    current_date = date.today()
    pitch_estimator_constants_fields = ["woba","woba_scale","woba_bb","woba_hbp","woba_single","woba_double","woba_triple","woba_home_run","fip_constant"]
    pitch_estimator_constants = {}
    mv_league_stats_averages_fields = ["pitch_count", "ip", "era", "whip", "fip_constant", "woba_pct", "x_woba", "hr_flyball_pct"]
    league_average_constants = {}

    woba_list = ['num_ab', 'num_bb', 'num_ibb', 'num_hbp', 'num_sacrifice_fly', 'num_1b', 'num_2b', 'num_3b', 'num_hr']
    leaderboard_kwargs = {
        "leaderboard" : fields.Str(required=False, missing="pitcher", validate=validate.OneOf(["pitcher", "pitch", "hitter"])),
        "tab" : fields.Str(required=False, missing="overview", validate=validate.OneOf(["overview", "standard", "statcast", "batted_ball", "batted_ball_2", "approach", "plate_discipline","projections"])),
        "handedness": fields.Str(required=False, missing="NA", validate=validate.OneOf(["R","L","NA"])),
        "opponent_handedness": fields.Str(required=False, missing="NA", validate=validate.OneOf(["R","L","NA"])),
        "league": fields.Str(required=False, missing="NA", validate=validate.OneOf(["AL","NL","NA"])),
        "division": fields.Str(required=False, missing="NA", validate=validate.OneOf(["East","Central","West","NA"])),
        "team": fields.Str(required=False, missing="NA", validate=validate.OneOf(valid_teams + ['NA'])),
        "home_away": fields.Str(required=False, missing="NA", validate=validate.OneOf(["Home","Away","NA"])),
        "year": fields.Str(required=False, missing='2021'),
        "month": fields.Str(required=False, missing="NA", validate=validate.OneOf(["1","2","3","4","5","6","7","8","9","10","11","12","NA"])),
        "half": fields.Str(required=False, missing="NA", validate=validate.OneOf(["First","Second","NA"])),
        "arbitrary_start": fields.Str(required=False, missing="NA"), #ISO date format
        "arbitrary_end": fields.Str(required=False, missing="NA") #ISO date format
    }

    def __init__(self, *args, **kwargs):

        self.current_date = date.today()
        self.current_year = self.current_date.year
        self.query_date = None
        self.query_year = None
        self.woba_year = None
        self.tab = 'overview'
        self.cols = {}
        self.stmt = ''

        # Lookup dicts
        # Available filter param (key) and col labels (value) 
        self.filter_fields = {
            'handedness': 'player_side',
            'opponent_handedness': 'player_side_against',
            'league': 'player_league',
            'home_away': 'player_home_away',
            'division': 'player_division', 
            'team': 'player_team_abb'
        }

        # Filter conditions assume the `=` operator unless specified as a syntax value in this list
        self.syntax_filters = ['game_played']
        
        # These are the columns for each leaderboard and tab.
        # [key][tab][col]
        # for 2022 pl7, we need:
        self.tab_display_fields = {
            "pitch": {
                "overview": ['num_pitches', 'num_starts', 'avg_velocity', 'usage_pct', 'o_swing_pct', 'zone_pct', 'swinging_strike_pct',
                            'called_strike_pct', 'csw_pct', 'put_away_pct', 'batting_average', 'num_pitches', 'strike_pct',
                            'plus_pct','groundball_pct', 'flyball_pct', 'woba', 'babip_pct', 'hr_flyball_pct', 'x_avg', 'x_woba',
                            'hard_pct', 'avg_spin_rate', 'num_pitches'],
                "statcast": ['num_pitches', 'num_starts', 'x_avg', 'x_woba', 'x_babip', 'x_wobacon', 'hard_pct', 'barrel_pct', 'ideal_bbe_pct', 'ideal_pa_pct', 
                            'average_launch_speed', 'average_fly_ball_launch_speed', 'average_launch_angle', 'avg_x_movement', 'avg_y_movement',
                            'avg_x_release', 'avg_y_release', 'avg_pitch_extension', 'avg_spin_rate', 'num_pitches'],
                "batted_ball": ['num_pitches', 'num_starts', 'groundball_pct', 'linedrive_pct', 'flyball_pct', 'infield_flyball_pct', 'weak_pct',
                                'medium_pct', 'hard_pct', 'pull_pct', 'opposite_field_pct', 'bacon_pct', 'num_pitches', 'foul_pct',
                                'hr_flyball_pct', 'center_pct', 'topped_pct', 'under_pct', 'flare_or_burner_pct', 'solid_pct', 'sweet_spot_pct'],
                "batted_ball_2": ['num_pitches', 'num_starts', 'num_pitches', 'hard_pct', 'barrel_pct', 'ideal_bbe_pct', 'ideal_pa_pct', 'batting_average', 'woba', 
                            'babip_pct', 'bacon_pct', 'x_avg', 'x_woba', 'x_babip', 'x_wobacon', 'average_launch_speed', 'average_fly_ball_launch_speed',
                            'average_launch_angle'],
                "plate_discipline": ['num_pitches', 'num_starts', 'o_swing_pct', 'zone_pct', 'swinging_strike_pct', 'called_strike_pct', 'csw_pct', 'strike_pct', 'plus_pct',
                                    'whiff_pct', 'swing_pct', 'contact_pct', 'z_contact_pct', 'o_contact_pct',
                                    'early_called_strike_pct', 'late_o_swing_pct', 'f_strike_pct', 'true_f_strike_pct', 'num_pitches'],
                "approach": ['num_pitches', 'num_starts', 'armside_pct','horizonal_middle_location_pct', 'gloveside_pct', 'high_pct',
                            'vertical_middle_location_pct', 'low_pct', 'heart_pct', 'inside_pct', 'outside_pct', 'early_pct','behind_pct', 'late_pct',
                            'zone_pct', 'non_bip_strike_pct', 'early_bip_pct', 'put_away_pct', 'num_pitches'],                
                "standard": ['num_pitches', 'num_starts', 'num_pitches', 'num_pa', 'num_hit', 'num_1b', 'num_2b', 'num_3b', 'num_hr', 'num_k', 'num_bb', 'num_hbp', 'num_wp', 
                            'num_strikes', 'num_balls', 'batting_average', 'slug_pct', 'woba'],
            },
            "pitcher": {
                "games_overview_standard":["wins", "losses", "num_games", "sho", "cg", "num_ip", "qs", "holds", "saves", "era", "lob_pct", "fip", "x_fip"],
                "overview": ['num_pitches', 'num_starts', "x_era", "num_hits_per_nine",
                            'whip', 'strikeout_pct', 'walk_pct', 'swinging_strike_pct', 'csw_pct',
                            'put_away_pct', 'babip_pct', 'hr_flyball_pct', 'plus_pct',
                            'x_babip', 'hard_pct', 'groundball_pct', 'swinging_strike_pct', 'csw_pct'],
                "standard": ['num_pitches', 'num_starts', 'num_hit', 'num_hr', 'num_k', 'num_bb',# 'ipg', 'ppg',
                            'num_hbp', 'num_wp', 'num_runs',
                            'num_1b', 'num_2b', 'num_3b', 'num_hr', 'num_ibb', 'num_bb', 'num_k', 'num_earned_runs'],
                "statcast": ['num_pitches', 'num_starts', 'strikeout_pct', 'walk_pct', 'batting_average', 'slug_pct', 'on_base_pct', 'woba', 'babip_pct', 'bacon_pct',
                            'x_avg', 'x_slug_pct', 'x_woba', 'x_babip', 'x_wobacon', 'x_era', 'average_launch_speed', 'average_launch_angle', 'ops_pct'],
                "batted_ball": ['num_pitches', 'num_starts', 'groundball_pct', 'linedrive_pct', 'flyball_pct', 'infield_flyball_pct', 'weak_pct',
                                'medium_pct', 'hard_pct', 'pull_pct', 'opposite_field_pct', 'bacon_pct', 'num_pitches', 'foul_pct',
                                'hr_flyball_pct', 'center_pct', 'topped_pct', 'under_pct', 'flare_or_burner_pct', 'solid_pct', 'sweet_spot_pct'],
                "batted_ball_2": ['num_pitches', 'num_starts','num_pitches', 'hard_pct', 'barrel_pct', 'ideal_bbe_pct', 'ideal_pa_pct', 'batting_average', 'woba', 
                            'babip_pct', 'bacon_pct', 'x_avg', 'x_woba', 'x_babip', 'x_wobacon', 'average_launch_speed', 'average_fly_ball_launch_speed',
                            'average_launch_angle'],
                "plate_discipline": ['num_pitches', 'num_starts', 'o_swing_pct', 'zone_pct', 'swinging_strike_pct', 'called_strike_pct', 'csw_pct', 'plus_pct',
                                    'contact_pct', 'z_contact_pct', 'o_contact_pct', 'swing_pct', 'strike_pct', 'early_called_strike_pct',
                                    'late_o_swing_pct', 'f_strike_pct', 'true_f_strike_pct'],
                "approach": ['num_pitches', 'num_starts', 'armside_pct','horizonal_middle_location_pct', 'gloveside_pct', 'high_pct',
                            'vertical_middle_location_pct', 'low_pct', 'heart_pct', 'inside_pct', 'outside_pct', 'early_pct','behind_pct', 'late_pct',
                            'zone_pct', 'non_bip_strike_pct', 'early_bip_pct', 'put_away_pct', 'num_pitches']                
            },
            "hitter": {
                "overview": ['num_games_played', 'num_runs_scored', 'num_rbi', 'num_sb', 'num_cs', 'num_pa', 'num_hr', 'num_runs', 'num_rbi',  'batting_average', 'on_base_pct', 'babip_pct', 'hr_flyball_pct',
                            'swinging_strike_pct', 'woba', 'strikeout_pct', 'walk_pct',  'slug_pct', 'x_avg', 'x_woba', 
                            'flyball_pct','groundball_pct', 'ideal_pa_pct', 'hard_pct', 'barrel_pct', 'csw_pct'],
                "standard": ['num_pa', 'num_hit', 'num_1b', 'num_2b', 'num_3b', 'num_hr', 'num_k', 'num_bb', 
                            'num_sb', 'num_cs', 'batting_average', 'on_base_pct', 'woba', 'num_hard_bip', 'num_barrel','num_hbp'],
                "statcast":['num_pa', 'strikeout_pct', 'walk_pct', 'batting_average', 'slug_pct' , 'on_base_pct', 'woba', 'babip_pct', 'bacon_pct', 'x_avg', 
                            'x_slug_pct', 'x_woba', 'x_babip', 'x_wobacon', 'max_launch_speed', 'average_launch_speed',
                            'average_fly_ball_launch_speed', 'average_launch_angle', 'ops_pct'], 
                "batted_ball": ['groundball_pct', 'linedrive_pct', 'flyball_pct', 'infield_flyball_pct', 'pull_pct', 'opposite_field_pct',
                                'babip_pct', 'bacon_pct', 'num_pa','center_pct', 'foul_pct'],
                "batted_ball_2": ['num_pa', 'weak_pct','topped_pct', 'under_pct', 'flare_or_burner_pct', 'solid_pct', 'barrel_pct', 'hard_pct',
                             'sweet_spot_pct','ideal_bbe_pct', 'ideal_pa_pct', 'max_launch_speed', 'average_launch_speed', 'average_fly_ball_launch_speed', 'average_launch_angle'],
                "plate_discipline": ['o_swing_pct', 'zone_pct', 'swinging_strike_pct', 'called_strike_pct', 'csw_pct',
                                    'contact_pct', 'z_contact_pct', 'o_contact_pct', 'swing_pct', 'early_called_strike_pct', 'strike_pct', 
                                    'late_o_swing_pct', 'f_strike_pct', 'true_f_strike_pct', 'num_pa', 'plus_pct'],
                "approach": ['inside_pct', 'horizonal_middle_location_pct', 'outside_pct', 'high_pct',
                            'vertical_middle_location_pct', 'low_pct', 'heart_pct', 'fastball_pct',
                            'early_secondary_pct', 'late_secondary_pct', 'zone_pct', 'non_bip_strike_pct',
                            'early_bip_pct', 'num_pa', 'early_pct', 'behind_pct', 'late_pct']
                
            }
        }

        # These are our aggregate field lookups that we use to dynamically generate the SQL stmt
        # [col][agg sql]
        self.aggregate_fields = {
            "num_games": "sum(g)",
            # "num_starts_lb": "sum(gs)",
            "sho": "sum(sho)",
            "cg": "sum(cg)",
            "qs": "sum(qs)",
            "holds": "sum(hold)",
            "saves": "sum(save)",
            # "ppg": 0,
            # "ipg": "ROUND(NULLIF(SUM(num_outs) / 3, 0), 1) / sum(g)",
            "lob_pct": "ROUND(COALESCE(SUM(hits + bb + ibb + hbp - runs)/NULLIF(SUM(hits + bb + ibb + hbp - (1.4 * home_run)),0)),3)",
            "num_hits_per_nine": "ROUND(COALESCE(9 * SUM(num_hit) / (ROUND(NULLIF(SUM(num_outs) / 3, 0), 1))),2)",
            "fip": "ROUND(COALESCE((13 * SUM(home_run) + 3 * SUM(bb + ibb) - 2 * SUM(strikeouts) )/(ROUND(NULLIF(SUM(outs) / 3, 0), 1)) + MAX(fip_constant)), 3)",
            # hr_flyball_pct is a league average/constant from the averages table
            "x_fip": "ROUND((COALESCE(((13 * (SUM(fly_ball) * MAX(hr_flyball_pct)/100)) + (3 * SUM(bb + ibb)) - (2 * SUM(strikeouts)))/ROUND(NULLIF(SUM(outs) / 3, 0), 1)) + MAX(fip_constant)), 3)",
            # "losses": "sum(num_loss)",
            # "wins": "sum(num_win)",
            # specific to lb - this is from the mv_pitcher_game_stat_for_leaderboard mv
            "losses": "sum(loss)",
            "wins": "sum(win)",
            # "num_pitches_lb": "sum(pitches)",
            "num_ip": "ROUND(SUM(outs::numeric) / 3, 1)",
            # "whip_lb": "ROUND((SUM(hits) + SUM(bb)) / NULLIF(SUM(outs) / 3, 0), 2)",
            "era": "ROUND(COALESCE(SUM(er::numeric), 0::bigint)::numeric / NULLIF(SUM(outs::numeric) / 3.0, 0::numeric) * 9.0, 2)",
            "x_era": "ROUND(COALESCE(_self.league_average_constants.get('era')_ * ( ( round((max(woba_bb) * sum(num_bb - num_ibb) + max(woba_hbp) * sum(num_hbp) + max(woba_single) * sum(num_xsingle) + max(woba_double) * sum(num_xdouble) + max(woba_triple) * sum(num_xtriple) + max(woba_home_run) * sum(num_xhomerun)) / NULLIF(sum(num_ab) + sum(num_bb) - sum(num_ibb) + sum(num_sacrifice_fly) + sum(num_hbp), 0), 3) / _self.league_average_constants.get('x_woba')_ ) ^ 2 )), 3)",
            # columns for hitter overview that is not at a pitch level
            "num_games_played": "totals.games",
            "num_rbi": "totals.rbi",
            "num_runs_scored": "totals.runs",
            "num_sb": "totals.sb",
            "num_cs": "totals.cs",
            "num_earned_runs":"COALESCE(SUM(num_runs::numeric), 0)",
            "num_starts": "COALESCE(start.num_starts, 0)",
            # Columns broken up by pitch
            "ops_pct": "ROUND((SUM(num_hit) + SUM(num_bb) + SUM(num_hbp)) / NULLIF((SUM(num_ab) + SUM(num_bb) + SUM(num_sacrifice_fly) + SUM(num_hbp)), 0), 3) + round((sum(num_1b) + 2::numeric * sum(num_2b) + 3::numeric * sum(num_3b) + 4::numeric * sum(num_hr)) / NULLIF(sum(num_ab), 0::numeric), 3)",
            "max_launch_speed": "round(max(max_launch_speed), 1)",
            "x_slug_pct": "round((sum(num_xsingle) + 2::numeric * sum(num_xdouble) + 3::numeric * sum(num_xtriple) + 4::numeric * sum(num_xhomerun)) / NULLIF(sum(num_statcast_at_bat), 0::numeric), 3)",
            "slug_pct": "round((sum(num_1b) + 2::numeric * sum(num_2b) + 3::numeric * sum(num_3b) + 4::numeric * sum(num_hr)) / NULLIF(sum(num_ab), 0::numeric), 3)",
            "num_strikes": "SUM(num_strikes)",
            "num_balls": "SUM(num_balls)",
            "num_wp": "SUM(num_wp)",
            "put_away_pct": "round(100::numeric * (sum(num_put_away) / NULLIF(sum(num_late), 0::numeric)), 1)",
            "whiff_pct": "round(100::numeric * (sum(num_whiff) / NULLIF(sum(num_swing), 0::numeric)), 1)",
            "topped_pct": "round(100::numeric * (sum(num_topped) / NULLIF(sum(num_statcast_bbe), 0::numeric)), 1)",
            "under_pct": "round(100::numeric * (sum(num_under) / NULLIF(sum(num_statcast_bbe), 0::numeric)), 1)",
            "flare_or_burner_pct": "round(100::numeric * (sum(num_flare_or_burner) / NULLIF(sum(num_statcast_bbe), 0::numeric)), 1)",
            "solid_pct": "round(100::numeric * (sum(num_solid) / NULLIF(sum(num_statcast_bbe), 0::numeric)), 1)",
            "sweet_spot_pct": "round(100::numeric * (sum(num_sweet_spot) / NULLIF(sum(num_statcast_bbe), 0::numeric)), 1)",
            "bacon_pct": "round(sum(num_hit) / NULLIF(sum(num_ab) - sum(num_k) + sum(num_sacrifice_fly), 0::numeric), 3)",
            "x_babip": "round((sum(num_xhit) - sum(num_xhomerun)) / NULLIF(sum(num_statcast_at_bat) - sum(num_xhomerun) - sum(num_k) + sum(num_sacrifice_fly), 0::numeric), 3)",
            "x_wobacon": "round((max(woba_bb) * sum(num_bb - num_ibb) + max(woba_hbp) * sum(num_hbp) + max(woba_single) * sum(num_xsingle) + max(woba_double) * sum(num_xdouble) + max(woba_triple) * sum(num_xtriple) + max(woba_home_run) * sum(num_xhomerun)) / NULLIF(sum(num_statcast_at_bat) + sum(num_bb) - sum(num_ibb) + sum(num_sacrifice_fly) + sum(num_hbp), 0::numeric), 3)",
            "ideal_bbe_pct": "round(100::numeric * (sum(num_ideal) / NULLIF(sum(num_batted_ball_event), 0::numeric)), 1)",
            "ideal_pa_pct":  "round(100::numeric * (sum(num_ideal) / NULLIF(sum(num_pa), 0::numeric)), 1)",
            "average_launch_speed": "round(sum(num_launch_speed) / NULLIF(sum(num_statcast_bbe), 0::numeric), 1)",
            "average_fly_ball_launch_speed": "round(sum(num_fly_ball_launch_speed) / NULLIF(sum(num_fly_ball), 0::numeric), 1)",
            "average_launch_angle": "round(sum(num_launch_angle) / NULLIF(sum(num_statcast_bbe), 0::numeric), 1)",
            "avg_x_movement": "round(sum(num_x_movement) / NULLIF(sum(location_counter), 0::numeric), 1)",
            "avg_y_movement": "round(sum(num_y_movement) / NULLIF(sum(location_counter), 0::numeric), 1)",
            "avg_x_release": "round(sum(num_x_release) / NULLIF(sum(location_counter), 0::numeric), 1)",
            "avg_y_release": "round(sum(num_y_release) / NULLIF(sum(location_counter), 0::numeric), 1)",
            "avg_pitch_extension": "round(sum(num_pitch_extension) / NULLIF(sum(pitch_extension_counter), 0::numeric), 1)",
            "avg_spin_rate": "round(sum(num_spin_rate) / NULLIF(sum(spin_rate_counter), 0::numeric), 1)",
            "woba": "round((max(woba_bb) * sum(num_bb - num_ibb) + max(woba_hbp) * sum(num_hbp) + max(woba_single) * sum(num_1b) + max(woba_double) * sum(num_2b) + max(woba_triple) * sum(num_3b) + max(woba_home_run) * sum(num_hr)) / NULLIF(sum(num_ab) + sum(num_bb) - sum(num_ibb) + sum(num_sacrifice_fly) + sum(num_hbp), 0), 3)",
            "x_woba": "round((max(woba_bb) * sum(num_bb - num_ibb) + max(woba_hbp) * sum(num_hbp) + max(woba_single) * sum(num_xsingle) + max(woba_double) * sum(num_xdouble) + max(woba_triple) * sum(num_xtriple) + max(woba_home_run) * sum(num_xhomerun)) / NULLIF(sum(num_statcast_at_bat) + sum(num_bb) - sum(num_ibb) + sum(num_sacrifice_fly) + sum(num_hbp), 0), 3)",
            "x_avg": "ROUND(SUM(num_xhit) / NULLIF(SUM(num_statcast_at_bat), 0), 3)",
            "num_pitches": "SUM(base.num_pitches)",
            "avg_velocity": "ROUND(SUM(total_velo) / NULLIF(SUM(num_velo), 0), 1)",
            "barrel_pct": "ROUND(100 * (SUM(num_barrel) / NULLIF(SUM(num_statcast_bbe), 0)), 1)",
            "foul_pct": "ROUND(100 * (SUM(num_foul) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "plus_pct": "ROUND(100 * (SUM(num_plus_pitch) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "first_pitch_swing_pct": "ROUND(100 * (SUM(num_first_pitch_swing) / NULLIF(SUM(num_first_pitch), 0)), 1)",
            "early_o_contact_pct": "ROUND(100 * (SUM(num_early_o_contact) / NULLIF(SUM(num_early), 0)), 1)",
            "late_o_contact_pct": "ROUND(100 * (SUM(num_late_o_contact) / NULLIF(SUM(num_late), 0)), 1)",
            #"avg_launch_speed": "ROUND((SUM(total_launch_speed) / NULLIF(SUM(num_launch_speed), 0))::DECIMAL, 1)",
            #"avg_launch_angle": "ROUND((SUM(total_launch_angle) / NULLIF(SUM(num_launch_angle), 0))::DECIMAL, 1)",
            #"avg_release_extension": "ROUND((SUM(total_release_extension) / NULLIF(SUM(num_release_extension), 0))::DECIMAL, 1)",
            #"avg_spin_rate": "round(sum(num_spin_rate) / NULLIF(sum(spin_rate_counter), 0)::DECIMAL, 1)",
            #"avg_x_movement": "ROUND((SUM(total_x_movement) / NULLIF(SUM(num_x_movement), 0))::DECIMAL, 1)",
            #"avg_z_movement": "ROUND((SUM(total_z_movement) / NULLIF(SUM(num_z_movement), 0))::DECIMAL, 1)",
            "armside_pct": "ROUND(100 * (SUM(num_armside) / NULLIF(SUM(location_counter), 0)), 1)",
            "babip_pct": "ROUND(((SUM(num_hit) - SUM(num_hr)) / NULLIF((sum(num_ab) - SUM(num_hr) - SUM(num_k) + SUM(num_sacrifice_fly)), 0)), 3)",
            "gloveside_pct": "ROUND(100 * (SUM(num_gloveside) / NULLIF(SUM(location_counter), 0)), 1)",
            "inside_pct": "ROUND(100 * (SUM(num_inside) / NULLIF(SUM(location_counter), 0)), 1)",
            "outside_pct": "ROUND(100 * (SUM(num_outside) / NULLIF(SUM(location_counter), 0)), 1)",
            "high_pct": "ROUND(100 * (SUM(num_high) / NULLIF(SUM(location_counter), 0)), 1)",
            "horizonal_middle_location_pct": "ROUND(100 * (SUM(num_horizontal_middle) / NULLIF(SUM(location_counter), 0)), 1)",
            "vertical_middle_location_pct": "ROUND(100 * (SUM(num_middle) / NULLIF(SUM(location_counter), 0)), 1)",
            "low_pct": "ROUND(100 * (SUM(num_low) / NULLIF(SUM(location_counter), 0)), 1)",
            "heart_pct": "ROUND(100 * (SUM(num_heart) / NULLIF(SUM(location_counter), 0)), 1)",
            "early_pct": "ROUND(100 * (SUM(num_early) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "behind_pct": "ROUND(100 * (SUM(num_behind) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "late_pct": "ROUND(100 * (SUM(num_late) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "zone_pct": "ROUND(100 * (SUM(num_zone) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "non_bip_strike_pct": "ROUND(100 * (SUM(num_non_bip_strike) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "early_bip_pct": "ROUND(100 * (SUM(num_early_bip) / NULLIF(SUM(base.num_early), 0)), 1)",
            "groundball_pct": "ROUND(100 * (SUM(num_ground_ball) / NULLIF(SUM(num_statcast_bbe), 0)), 1)",
            "linedrive_pct": "ROUND(100 * (SUM(num_line_drive) / NULLIF(SUM(num_statcast_bbe), 0)), 1)",
            "flyball_pct": "ROUND(100 * (SUM(num_fly_ball) / NULLIF(SUM(num_statcast_bbe), 0)), 1)",
            "infield_flyball_pct": "ROUND(100 * (SUM(num_if_fly_ball) / NULLIF(SUM(num_statcast_bbe), 0)), 1)",
            "weak_pct": "ROUND(100 * (SUM(num_weak_bip) / NULLIF(SUM(num_statcast_bbe), 0)), 1)",
            "medium_pct": "ROUND(100 * (SUM(num_medium_bip) / NULLIF(SUM(num_statcast_bbe), 0)), 1)",
            # HH %
            # In an upcoming update, this value needs to be converted to have num_statcast_bbe as the denominator, then send hard_contact_pct to the WP team instead of hard_pct
            "hard_pct": "ROUND(100 * (SUM(num_hard_bip) / NULLIF(SUM(num_statcast_plate_appearance), 0)), 1)", 
            # HC %
            "hard_contact_pct": "ROUND(100 * (SUM(num_hard_bip) / NULLIF(SUM(num_statcast_plate_appearance), 0)), 1)",
            "pull_pct": "ROUND(100 * (SUM(num_pulled_bip) / NULLIF(SUM(num_statcast_bbe), 0)), 1)",
            "center_pct": "ROUND(100 * (SUM(num_center_bip) / NULLIF(SUM(num_statcast_bbe), 0)), 1)",
            "opposite_field_pct": "ROUND(100 * (SUM(num_opposite_bip) / NULLIF(SUM(num_statcast_bbe), 0)), 1)",
            "swing_pct": "ROUND(100 * (SUM(num_swing) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "o_swing_pct": "ROUND(100 * (SUM(num_o_swing) / NULLIF(SUM(base.num_pitches - num_zone), 0)), 1)",
            "z_swing_pct": "ROUND(100 * (SUM(num_z_swing) / NULLIF(SUM(num_zone), 0)), 1)",
            "contact_pct": "ROUND(100 * (SUM(num_contact) / NULLIF(SUM(num_swing), 0)), 1)",
            "o_contact_pct": "ROUND(100 * (SUM(num_o_contact) / NULLIF(SUM(num_o_swing), 0)), 1)",
            "z_contact_pct": "ROUND(100 * (SUM(num_z_contact) / NULLIF(SUM(num_z_swing), 0)), 1)",
            "swinging_strike_pct": "ROUND(100 * (SUM(num_whiff) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "called_strike_pct": "ROUND(100 * (SUM(num_called_strike) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "csw_pct": "ROUND(100 * (SUM(num_called_strike_plus_whiff) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "strike_pct": "ROUND(100 * (SUM(num_strikes) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "early_called_strike_pct": "ROUND(100 * (SUM(num_early_called_strike) / NULLIF(SUM(num_early), 0)), 1)",
            "late_o_swing_pct": "ROUND(100 * (SUM(num_late_o_swing) / NULLIF(SUM(num_late), 0)), 1)",
            "f_strike_pct": "ROUND(100 * (SUM(num_first_pitch_strike) / NULLIF(SUM(num_first_pitch), 0)), 1)",
            "true_f_strike_pct": "ROUND(100 * (SUM(num_true_first_pitch_strike) / NULLIF(SUM(num_first_pitch), 0)), 1)",
            "put_away_pct": "ROUND(100 * (SUM(num_put_away) / NULLIF(SUM(num_late), 0)), 1)",
            "batting_average": "ROUND(SUM(num_hit) / NULLIF(SUM(num_ab), 0), 3)",
            "strikeout_pct": "ROUND(100 * (SUM(num_k) / NULLIF(SUM(num_pa), 0)), 1)",
            "walk_pct": "ROUND(100 * (SUM(num_bb) / NULLIF(SUM(num_pa), 0)), 1)",
            "hr_flyball_pct": "ROUND(100 * (SUM(num_hr) / NULLIF(SUM(num_fly_ball), 0)), 3)",
            "on_base_pct": "ROUND((SUM(num_hit) + SUM(num_bb) + SUM(num_hbp)) / NULLIF((SUM(num_ab) + SUM(num_bb) + SUM(num_sacrifice_fly) + SUM(num_hbp)), 0), 3)",
            "whip": "ROUND((SUM(num_hit) + SUM(num_bb)) / NULLIF(SUM(num_outs) / 3, 0), 2)",
            # "num_ip": "ROUND(NULLIF(SUM(num_outs) / 3, 0), 1)",
            "num_zone": "SUM(num_zone)",
            "total_velo": "SUM(total_velo)",
            "num_velo": "SUM(num_velo)",
            "num_armside": "SUM(num_armside)",
            "num_gloveside": "SUM(num_gloveside)",
            "num_inside": "SUM(num_inside)",
            "num_outside": "SUM(num_outside)",
            "num_horizontal_middle": "SUM(num_horizontal_middle)",
            "num_high": "SUM(num_high)",
            "num_middle": "SUM(num_middle)",
            "num_low": "SUM(num_low)",
            "num_heart": "SUM(num_heart)",
            "num_early": "SUM(num_early)",
            "num_late": "SUM(num_late)",
            "num_behind": "SUM(num_behind)",
            "num_non_bip_strike": "SUM(num_non_bip_strike)",
            "num_batted_ball_event": "SUM(num_batted_ball_event)",
            "num_early_bip": "SUM(num_early_bip)",
            "num_fastball": "SUM(num_fastball)",
            "num_secondary": "SUM(num_secondary)",
            "num_early_secondary": "SUM(num_early_secondary)",
            "num_late_secondary": "SUM(num_late_secondary)",
            "num_called_strike": "SUM(num_called_strike)",
            "num_early_called_strike": "SUM(num_early_called_strike)",
            "num_called_strike_plus_whiff": "SUM(num_called_strike_plus_whiff)",
            "num_put_away": "SUM(num_put_away)",
            "num_swing": "SUM(num_swing)",
            "num_whiff": "SUM(num_whiff)",
            "num_contact": "SUM(num_contact)",
            "num_foul": "SUM(num_foul)",
            "num_first_pitch_swing": "SUM(num_first_pitch_swing)",
            "num_first_pitch_strike": "SUM(num_first_pitch_strike)",
            "num_true_first_pitch_strike": "SUM(num_true_first_pitch_strike)",
            "num_plus_pitch": "SUM(num_plus_pitch)",
            "num_z_contact": "SUM(num_z_contact)",
            "num_o_swing": "SUM(num_o_swing)",
            "num_o_contact": "SUM(num_o_contact)",
            "num_early_o_swing": "SUM(num_early_o_swing)",
            "num_early_o_contact": "SUM(num_early_o_contact)",
            "num_late_o_swing": "SUM(num_late_o_swing)",
            "num_late_o_contact": "SUM(num_late_o_contact)",
            "num_pulled_bip": "SUM(num_pulled_bip)",
            "num_opposite_bip": "SUM(num_opposite_bip)",
            "num_line_drive": "SUM(num_line_drive)",
            "num_fly_ball": "SUM(num_fly_ball)",
            "num_if_fly_ball": "SUM(num_if_fly_ball)",
            "num_ground_ball": "SUM(num_ground_ball)",
            "num_weak_bip": "SUM(num_weak_bip)",
            "num_medium_bip": "SUM(num_medium_bip)",
            "num_hard_bip": "SUM(num_hard_bip)",
            "num_outs": "SUM(num_outs)",
            "num_pa": "SUM(num_pa)",
            "num_ab": "SUM(num_ab)",
            "num_1b": "SUM(num_1b)",
            "num_2b": "SUM(num_2b)",
            "num_3b": "SUM(num_3b)",
            "num_hr": "SUM(num_hr)",
            "num_bb": "SUM(num_bb)",
            "num_ibb": "SUM(num_ibb)",
            "num_hbp": "SUM(num_hbp)",
            "num_sacrifice": "SUM(num_sacrifice)",
            "num_sacrifice_fly": "SUM(num_sacrifice_fly)",
            "num_sacrifice_hit": "SUM(num_sacrifice_hit)",
            "num_k": "SUM(num_k)",
            "num_hit": "SUM(num_hit)",
            "num_runs": "SUM(num_runs)",
            "num_barrel": "SUM(num_barrel)",
            "total_launch_speed": "SUM(total_launch_speed)",
            "num_launch_speed": "SUM(num_launch_speed)",
            "total_launch_angle": "SUM(total_launch_angle)",
            "num_launch_angle": "SUM(num_launch_angle)",
            "total_release_extension": "SUM(total_release_extension)",
            "num_release_extension": "SUM(num_release_extension)",
            "total_spin_rate": "SUM(total_spin_rate)",
            "num_spin_rate": "SUM(num_spin_rate)",
            "total_x_movement": "SUM(total_x_movement)",
            "num_x_movement": "SUM(num_x_movement)",
            "total_z_movement": "SUM(total_z_movement)",
            "num_z_movement": "SUM(num_z_movement)",
            "usage_pct": "ROUND(100 * (SUM(base.num_pitches) / NULLIF(pldp.num_pitches, 0)), 1)",
            "fastball_pct": "ROUND(100 * (SUM(num_fastball) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "early_secondary_pct": "ROUND(100 * (SUM(num_early_secondary) / NULLIF(SUM(num_early), 0)), 1)",
            "late_secondary_pct": "ROUND(100 * (SUM(num_late_secondary) / NULLIF(SUM(num_late), 0)), 1)",
        }

    # Error handler is necessary for webargs and flask-restful to work together
    @parser.error_handler
    def handle_request_parsing_error(err, req, schema, error_status_code, error_headers):
        abort(error_status_code, errors=err.messages)
        
    def get_lb_projections(self, kwargs):

        # check hitter or pitcher

        lb_type = kwargs.get('leaderboard')

        if(lb_type == "pitcher"):
            reliever_query = """select dr.mlbid as player_id,
                                    dr."name" as player_name,
                                    teams.team_name as player_team,
                                    dr.team as player_team_abbr,
                                    dr.g as num_games,
                                    dr.gs as num_starts,
                                    dr.ip as num_ip,
                                    dr.qs,
                                    dr.w as wins,
                                    dr.l as losses,
                                    dr.sv as saves,
                                    dr.hld as holds,
                                    dr.era,
                                    dr.whip,
                                    dr.h as num_hit,
                                    dr.r as runs,
                                    dr.hr as hrs,
                                    dr.sop as strikeout_pct,
                                    dr.bbp as walk_pct,
                                    dr.so as strikeouts,
                                    dr.bb as walks
                                from dfs_2022_relievers dr, teams teams
                                where dr.team = teams.abbreviation"""

            starter_query = """select ds.mlbid as player_id,
                                    ds."name" as player_name,
                                    teams.team_name as player_team,
                                    ds.team as player_team_abbr,
                                    ds.g as num_games,
                                    ds.gs as num_starts,
                                    ds.ip as num_ip,
                                    ds.w as wins,
                                    ds.l as losses,
                                    ds.wp,
                                    ds.qs,
                                    ds.era,
                                    ds.whip,
                                    ds.h as num_hit,
                                    ds.r as runs,
                                    ds.hr as hrs,
                                    ds.sop as strikeout_pct,
                                    ds.bbp as walk_pct,
                                    ds.so as strikeouts,
                                    ds.bb as walks
                                from dfs_2022_starters ds, teams teams
                                where ds.team = teams.abbreviation"""

            reliever_df = fetch_dataframe(reliever_query)
            starter_df = fetch_dataframe(starter_query)

            pitcher_df = pd.concat(
                objs = [reliever_df, starter_df],
                join = "outer"
            )

            print(pitcher_df)
            for i, row in pitcher_df.iterrows():

                pitcher_df.at[i, "era"] = "{:.2f}".format(row.get("era"))
                pitcher_df.at[i, "whip"] = "{:.2f}".format(row.get("whip"))
                pitcher_df.at[i, "wp"] = "{:.2f}".format(row.get("wp"))

            return json.loads(pitcher_df.to_json(orient='records'))

        elif(lb_type == "hitter"):
            
            query = """select 
                db.mlbid as player_id, 
                db."name" as player_name, 
                teams.team_name as player_team, 
                db.team as player_team_abbr,
                null as num_games,
                db.pa as num_pa, 
                db.h as num_hit, 
                db.s as num_1b,
                db.d as num_2b,
                db.t as num_3b,
                db.hr as num_hr, 
                db.r as num_run,
                db.rbi as num_rbi, 
                db.sb as num_stolen_base, 
                db.cs as caught_stealing,
                db.so as num_k, 
                db.bb as num_bb, 
                db."avg" as batting_average, 
                db.obp as on_base_percentage,
                db.slg as slugging,
                db.obp + db.slg as on_base_plus_slugging
            from dfs_2022_batters db, teams teams
            where db.team = teams.abbreviation"""

            df = fetch_dataframe(query)

            for i, row in df.iterrows():
                df.at[i, "on_base_percentage"] = "{:.3f}".format(row.get("on_base_percentage"))
                df.at[i, "batting_average"] = "{:.3f}".format(row.get("batting_average"))
                df.at[i, "slugging"] = "{:.3f}".format(row.get("slugging"))
                df.at[i, "on_base_plus_slugging"] = "{:.3f}".format(row.get("on_base_plus_slugging"))

            return json.loads(df.to_json(orient='records'))


    @use_kwargs(leaderboard_kwargs)
    def get(self, **kwargs):    

        # if we're getting projection data, just return projections immediately
        if kwargs.get('tab') == 'projections':
            return self.get_lb_projections(kwargs)

        start_year = None
        end_year = None

        #Query year start and end
        if ( kwargs['arbitrary_start'] != 'NA'):
            start_year = datetime.strptime(kwargs['arbitrary_start'], '%Y-%m-%d').strftime('%Y')
        
        if ( kwargs['arbitrary_end'] != 'NA' ):
            end_year = datetime.strptime(kwargs['arbitrary_end'], '%Y-%m-%d').strftime('%Y')

        # Set our query year so we can select from year specific smaller views 
        if ( start_year and end_year and start_year == end_year ):
            self.query_year = start_year
        elif kwargs['year'] != 'NA':
            self.query_year =kwargs['year']

        # Tab specific formats
        self.tab = kwargs['tab']
        # Set woba on overview tab
        if (self.tab == 'overview'):
            if (self.query_year):
                if (self.current_year == self.query_year):
                    self.woba_year = self.query_year - 1
                else:
                    self.woba_year = self.query_year
            else:
                if (end_year == self.current_year):
                    self.woba_year = end_year - 1
                else:
                    self.woba_year = end_year

        self.set_constants(self.query_year)  
        self.replace_constants()
        return self.fetch_result(kwargs.get('leaderboard'), **kwargs)

    def set_constants(self, year):
        
        pec_table = fetch_dataframe(f"select * from pitch_estimator_constants pec where year = {year}")
        for field in self.pitch_estimator_constants_fields:
            self.pitch_estimator_constants[field] = str(pec_table[field][0])

        mlsa_table = fetch_dataframe(f'select * from mv_league_stats_averages mlsa where mlsa.year_played = {year} and mlsa."position" = \'ALL\'')
        for field in self.mv_league_stats_averages_fields:
            self.league_average_constants[field] = str(mlsa_table[field][0])
        
        
    # this ugly method is used to replace any and all relevant constants that are available and set in the method above. This is a replacement for performing sub-selects
    def replace_constants(self):
        
        # self.aggregate_fields['fip'] = self.aggregate_fields['fip'].replace("_self.pitch_estimator_constants.get('fip_constant')_", self.pitch_estimator_constants.get('fip_constant'))
        
        self.aggregate_fields['x_era'] = self.aggregate_fields['x_era'].replace("_self.league_average_constants.get('era')_", self.league_average_constants.get('era'))
        self.aggregate_fields['x_era'] = self.aggregate_fields['x_era'].replace("_self.league_average_constants.get('x_woba')_", self.league_average_constants.get('x_woba'))

        # self.aggregate_fields['x_fip'] = self.aggregate_fields['x_fip'].replace("_self.league_average_constants.get('hr_flyball_pct')_", self.league_average_constants.get('hr_flyball_pct'))
        # self.aggregate_fields['x_fip'] = self.aggregate_fields['x_fip'].replace("_self.pitch_estimator_constants.get('fip_constant')_", self.pitch_estimator_constants.get('fip_constant'))

    def fetch_result(self, query_type, **query_args):
        # Caching wrapper for fetch_data
        result = None

        if (current_app.config.get('BYPASS_CACHE')):
            # Bypassing Caching of JSON Results
            result = self.fetch_data(query_type, **query_args)
        else:
            # Using Cache for JSON Results
            cache_key_date = json.dumps(query_args)
            cache_key_resource_type = self.__class__.__name__

            cache_key = f'{cache_key_resource_type}-{query_type}-{cache_key_date}'
            result = current_app.cache.get(cache_key)
            if (result is None):

                
                
                if query_args.get('leaderboard') == 'pitcher' and (query_args.get('tab') == 'overview' or query_args.get('tab') == "standard"):
                    result = self.handle_pitcher_overview_standard(**query_args)
                
                else:
                    result = self.fetch_data(query_type, **query_args)
                # Set Cache expiration to 5 mins
                current_app.cache.set(cache_key, result, 300)

        return result

    def handle_pitcher_overview_standard(self, **query_args):
        # fetch data from daily table: 
        daily = self.fetch_data('pitcher', return_dataframe=True, **query_args)

        # fetch data from mv_pitcher_game_stats_for_leaderboard
        query_args['tab'] = 'games_overview_standard'
        lb = self.fetch_data('pitcher',  return_dataframe=True, **query_args)

        merged_df = pd.merge(daily, lb, how='inner', left_on=['player_id'], right_on =['player_id'])

        player_team_query = "select t.abbreviation as player_team_abbr, t.team_name as player_team, p.mlb_player_id from players p, teams t where p.current_team_id = t.team_id "
        player_team_df = fetch_dataframe(player_team_query)
        merged_player_df = pd.merge(merged_df, player_team_df, how = 'left', left_on=['player_id'], right_on = ['mlb_player_id'])

        results = self.format_results("pitcher", merged_player_df)
        output = self.get_json("pitcher", results, **query_args)


        return output
        # join that data and return
        # return None
         
    def fetch_data(self, query_type, **query_args):
        query = self.get_query(query_type, **query_args)
        var_dump(query)
        cursor_list = self.build_cursor_execute_list(query_type, **query_args)
        raw = fetch_dataframe(query, cursor_list)

        #used for when we hit pitcher - overview or standard and need to concat results in python
        if(query_args.get("return_dataframe")):
            return raw

        ## same code as just above to tie a player team and player team abb to each player data object
        player_team_query = "select t.abbreviation as player_team_abbr, t.team_name as player_team, p.mlb_player_id from players p, teams t where p.current_team_id = t.team_id "
        player_team_df = fetch_dataframe(player_team_query)
        merged_player_df = pd.merge(raw, player_team_df, how = 'left', left_on=['player_id'], right_on = ['mlb_player_id'])

        results = self.format_results(query_type, merged_player_df)
        output = self.get_json(query_type, results, **query_args)

        return output

    def get_query(self, query_type, **query_args):

        # All leaderboards use dynamic sql generation created in v2.1
        # Refactored in v3 as resource class functions.

        # Get our base colums and labels
        self.cols = self.get_cols(**query_args)
        self.stmt = 'SELECT'
        for label, sql in self.cols.items():
            self.stmt = f"{self.stmt} {sql} AS {label},"
        
        # Strip trailing comman & space
        self.stmt = self.stmt[:-1]

        # Add table to select from

        table = self.get_table(query_args)
        self.stmt = f"{self.stmt} FROM {table} base"
        conditions = self.get_conditions(**query_args)
        groups = self.get_groups(**query_args)

        def default():
            return f"SELECT 'query not defined' AS error, '{query_type}' AS query;"

        def pitch():
            # Make a sub query for a join
            join_sql = self.get_joins(query_type, **query_args)
            self.stmt = f'{self.stmt} {join_sql}'

            if conditions:
                self.stmt = f"{self.stmt} WHERE"
                for col, val in conditions.items():
                    if (col in self.syntax_filters):
                        self.stmt = f'{self.stmt} {col} {val} AND'
                    else:
                        self.stmt = f"{self.stmt} {col} = '{val}' AND"

                self.stmt = self.stmt[:-3]

            self.stmt = f'{self.stmt} GROUP BY'

            for col in groups:
                self.stmt = f'{self.stmt} {col},'

            self.stmt = self.stmt[:-1]

            return self.stmt

        def pitcher():
            # Make a sub query for a join
            join_sql = self.get_joins(query_type, **query_args)
            self.stmt = f'{self.stmt} {join_sql}'

            if conditions:
                self.stmt = f"{self.stmt} WHERE"
                for col, val in conditions.items():
                    if (col in self.syntax_filters):
                        self.stmt = f'{self.stmt} {col} {val} AND'
                    else:
                        self.stmt = f"{self.stmt} {col} = '{val}' AND"

                self.stmt = self.stmt[:-3]

            self.stmt = f'{self.stmt} GROUP BY'

            for col in groups:
                self.stmt = f'{self.stmt} {col},'

            self.stmt = self.stmt[:-1]

            return self.stmt
        
        def hitter():

            join_sql = self.get_joins(query_type, **query_args)
            self.stmt = f'{self.stmt} {join_sql}'
            
            if conditions:
                self.stmt = f"{self.stmt} WHERE"
                for col, val in conditions.items():
                    if (col in self.syntax_filters):
                        self.stmt = f'{self.stmt} base.{col} {val} AND'
                    else:
                        self.stmt = f"{self.stmt} base.{col} = '{val}' AND"

                self.stmt = self.stmt[:-3]
            
            self.stmt = f'{self.stmt} GROUP BY'

            for col in groups:
                self.stmt = f'{self.stmt} {col},'

            self.stmt = self.stmt[:-1]

            return self.stmt

        def pitcher_games_overview_standard():

            if conditions:
                self.stmt = f"{self.stmt} WHERE"
                for col, val in conditions.items():
                    if (col in self.syntax_filters):
                        self.stmt = f'{self.stmt} {col} {val} AND'
                    else:
                        self.stmt = f"{self.stmt} {col} = '{val}' AND"

                self.stmt = self.stmt[:-3]

            self.stmt = f'{self.stmt} GROUP BY'

            for col in groups:
                self.stmt = f'{self.stmt} {col},'

            self.stmt = self.stmt[:-1]

            return self.stmt

            # return "SELECT pitchermlbamid as player_id, pitcherleague, pitcherdivision, year_played as year, month_played, half_played, pitcher_home_away, gs, g, win, loss, cg, sho, qs, save, hold, pitches, outs FROM mv_pitcher_game_stats_for_leaderboard WHERE year_played = '2021' GROUP BY pitchermlbamid, year_played,pitcherleague, pitcherdivision, month_played,half_played, pitcher_home_away,gs, g, win,loss,cg, sho,qs,save, hold, pitches, outs"

        queries = {
            "pitch": pitch,
            "pitcher": pitcher,
            "hitter": hitter,
            "pitcher_games_overview_standard": pitcher_games_overview_standard,
        }

        if query_args.get("tab") == "games_overview_standard":
            return queries.get("pitcher_games_overview_standard")()

        return queries.get(query_type, default)()

    def format_results(self, query_type, data):
        def default():
            return data

        def pitch():
            return data

        def pitcher():
            return data

        def hitter():
            return data

        formatting = {
            "pitch": pitch,
            "pitcher": pitcher,
            "hitter": hitter
        }

        return formatting.get(query_type, default)()
    
    def get_json(self, query_type, results, **query_args):
        
        def default():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)
            
            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(results.to_json(orient='records', date_format='iso'))

        def pitch():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)
            
            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(results.to_json(orient='records', date_format='iso'))
        
        def pitcher():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)
            
            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(results.to_json(orient='records', date_format='iso'))

        def hitter():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)
            
            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(results.to_json(orient='records', date_format='iso'))

        json_data = {
            "pitch": pitch,
            "pitcher": pitcher,
            "hitter": hitter
        }

        return json_data.get(query_type, default)()

    def build_cursor_execute_list(self, lb, **kwargs):
        cursor_list = list()
        args = [kwargs['year'], kwargs['month'], kwargs['half'], kwargs['arbitrary_start'], kwargs['arbitrary_end'], kwargs['handedness'], kwargs['opponent_handedness'], kwargs['league'], kwargs['division'], kwargs['team'], kwargs['home_away']]
        join_args = [kwargs['year'], kwargs['month'], kwargs['half'], kwargs['arbitrary_start'], kwargs['arbitrary_end'], kwargs['home_away']]

        if lb in ['pitch', 'pitcher']:
            for arg in join_args:
                if arg != 'NA':
                    cursor_list.append(arg)

        for arg in args:
            if arg != 'NA':
                cursor_list.append(arg)

        return cursor_list

    def get_cols(self, **kwargs):

        leaderboard = kwargs.get('leaderboard')

        def pitcher():

            fields = {
                'player_id': 'pitchermlbamid',
                'player_name': 'pitchername',
                # 'player_team': 'pitcherteam',
                'player_team_abb': 'pitcherteam_abb',
                'player_side': 'pitcherside',
                'player_side_against': 'hitterside',
                # 'player_league': 'pitcherleague',
                # 'player_division': 'pitcherdivision',
                'player_home_away': 'pitcher_home_away',
                'num_starts': 'COALESCE(start.num_starts, 0)'
            }

            for colname in self.tab_display_fields[leaderboard][self.tab]:
                # if colname == 'woba':
                #     for woba_variable in self.woba_list:
                #         fields[woba_variable] = self.aggregate_fields[woba_variable]
                # else:
                fields[colname] = self.aggregate_fields[colname]
                
                for filter, fieldname in self.filter_fields.items():
                    if kwargs[filter] == 'NA' and fieldname in fields:
                        del fields[fieldname]
            
            return fields
        
        def pitch():
            
            fields = {
                'player_id': 'pitchermlbamid',
                'player_name': 'pitchername',
                # 'player_team': 'pitcherteam',
                'player_team_abb': 'pitcherteam_abb',
                'player_side': 'pitcherside',
                'player_side_against': 'hitterside',
                # 'player_league': 'pitcherleague',
                # 'player_division': 'pitcherdivision',
                'player_home_away': 'pitcher_home_away',
                'pitchtype': 'pitchtype'
            }

            for colname in self.tab_display_fields[leaderboard][self.tab]:
                # if colname == 'woba':
                #     for woba_variable in self.woba_list:
                #         fields[woba_variable] = self.aggregate_fields[woba_variable]
                # else:
                fields[colname] = self.aggregate_fields[colname]

                for filter, fieldname in self.filter_fields.items():
                    if kwargs[filter] == 'NA' and fieldname in fields:
                        del fields[fieldname]
            
            return fields

        def hitter():

            fields = {
                'player_id': 'hittermlbamid',
                'player_name': 'hittername',
                # 'player_team': 'hitterteam',
                'player_team_abb': 'hitterteam_abb',
                'player_side': 'hitterside',
                'player_side_against': 'pitcherside',
                # 'player_league': 'hitterleague',
                # 'player_division': 'hitterdivision',
                'player_home_away': 'hitter_home_away',
            }

            for colname in self.tab_display_fields[leaderboard][self.tab]:
                # if colname == 'woba':
                #     for woba_variable in self.woba_list:
                #         fields[woba_variable] = self.aggregate_fields[woba_variable]
                # else:
                fields[colname] = self.aggregate_fields[colname]

                for filter, fieldname in self.filter_fields.items():
                    if kwargs[filter] == 'NA' and fieldname in fields:
                        del fields[fieldname]
            
            return fields
        
        def pitcher_games_overview_standard():

            fields = {
                "player_id": "pitchermlbamid",
                "player_home_away": "pitcher_home_away",
                # "player_division": "pitcherdivision",
                # "player_league": "pitcherleague"
            }

            for colname in self.tab_display_fields[leaderboard]['games_overview_standard']:
                # if colname == 'woba':
                #     for woba_variable in self.woba_list:
                #         fields[woba_variable] = self.aggregate_fields[woba_variable]
                # else:
                fields[colname] = self.aggregate_fields[colname]

                for filter, fieldname in self.filter_fields.items():
                    if kwargs[filter] == 'NA' and fieldname in fields:
                        del fields[fieldname]

            return fields

        cols = {
            "pitch": pitch,
            "pitcher": pitcher,
            "hitter": hitter,
            "pitcher_games_overview_standard": pitcher_games_overview_standard
        }

        if(kwargs.get("tab") == "games_overview_standard"):
            return cols.get("pitcher_games_overview_standard")()

        return cols.get(leaderboard, pitcher)()
    
    def get_table(self, query_args):
        
        if(query_args.get("tab") == "games_overview_standard"):
            return "mv_pitcher_game_stats_for_leaderboard"

        table = 'pl_leaderboard_daily'


        if (self.query_year):
            if(self.query_year in self.valid_years):
                table = f'{table}_{self.query_year}'
            else:
                raise InvalidUsage(status_code=404, message = f"Invalid year filtering: {self.query_year}", payload = {"valid_years": self.valid_years})
                

        return table

    def get_conditions(self, **kwargs):
        # global date filters
        stmts = {}

        if (kwargs['arbitrary_start'] != 'NA' and kwargs['arbitrary_end'] != 'NA'):
            stmts['game_played'] = f"BETWEEN {kwargs['arbitrary_start']} AND {kwargs['arbitrary_end']}"
        elif (kwargs['year'] != 'NA'):
            stmts['year_played'] = f"{kwargs['year']}"
        
        if (kwargs['month'] != 'NA'):
            stmts['month_played'] = f"{kwargs['month']}"

        if (kwargs['half'] in ['First','Second']):
            stmts['half_played'] = f"{kwargs['half']}"

        # Iterate though our filters. Leaderboard specific cols have been included via the `get_cols()` method
        # Add the corresponding cols to the WHERE conditions
        for filter, fieldname in self.filter_fields.items():
            if (kwargs[filter] != 'NA'):

                key = self.cols[fieldname]
                stmts[key] = f"{kwargs[filter]}"

        return stmts

    def get_groups(self, **kwargs):

        groupby = []
        # Iterate though our filters. Leaderboard specific cols have been included via the `get_cols()` method
        # Add the corresponding cols to the WHERE conditions
        
        for filter, fieldname in self.filter_fields.items():
            if (kwargs[filter] != 'NA'):
                groupby.append(fieldname)

        def pitch():
            # groupby_fields = ["pitchermlbamid", "pitchername", "pitcherteam", "pitcherteam_abb", "pitchtype", "pldp.num_pitches", "start.num_starts"]
            groupby_fields = ["pitchermlbamid", "pitchername", "pitchtype", "pldp.num_pitches", "start.num_starts"]
            for field in groupby_fields:
                groupby.append(field)

            return groupby

        def pitcher():
            # groupby_fields = ["base.pitchermlbamid", "pitchername", "pitcherteam", "pitcherteam_abb"]
            groupby_fields = ["base.pitchermlbamid", "pitchername","start.num_starts"]
            for field in groupby_fields:
                groupby.append(field)

            return groupby

        def hitter():

            # groupby_fields = ["hittermlbamid", "hittername", "hitterteam", "hitterteam_abb"]
            groupby_fields = ["hittermlbamid", "hittername"]

            include_sb_cs = ["overview", "standard"]
            # SB and CS are game totals are are not aggregated by hitter/pitcher interaction and must be grouped
            if kwargs.get('tab') in include_sb_cs:
                groupby_fields.append("num_sb")
                groupby_fields.append("num_cs")    

            include_games_runs_rbi = ["overview"]
            if kwargs.get('tab') in include_games_runs_rbi:
                groupby_fields.append("num_games_played")
                groupby_fields.append("num_runs_scored")
                groupby_fields.append("num_rbi")

            # groupby_fields = ["hittermlbamid", "hittername", "hitterteam", "hitterteam_abb"]
            for field in groupby_fields:
                groupby.append(field)

            return groupby

        def pitcher_games_overview_standard():

            # groupby_fields = ["pitchermlbamid", "year_played", "pitcherleague", "pitcherdivision"]
            groupby_fields = ["pitchermlbamid", "year_played"]

            for field in groupby_fields:
                groupby.append(field)

            return groupby

        groups = {
            "pitch": pitch,
            "pitcher": pitcher,
            "hitter": hitter,
            "pitcher_games_overview_standard": pitcher_games_overview_standard
        }

        if kwargs.get("tab") == "games_overview_standard":
            return groups.get("pitcher_games_overview_standard")()

        return groups.get(kwargs.get('leaderboard'), pitcher)()

    def get_joins(self, lb, **kwargs):
        def default():
            return None

        def pitch():
            stmt = self.get_joins('pitcher',**kwargs)
            table = self.get_table(kwargs)

            stmt = f"{stmt} JOIN ( SELECT pitchermlbamid AS player_id, SUM(num_pitches) AS num_pitches FROM {table} sub"
            groupby = ''

            conditions = self.get_conditions(**kwargs)
            if conditions.get('hitterside'):
                conditions.pop('hitterside')
            if conditions.get('pitcherside'):
                conditions.pop('pitcherside')    

            if conditions:
                stmt = f"{stmt} WHERE"
                for col, val in conditions.items():
                    if (col in self.syntax_filters):
                        stmt = f"{stmt} {col} {val} AND"
                    else:
                        stmt = f"{stmt} {col} = '{val}' AND"
                        
                    groupby = f"{groupby}, {col}"


                stmt = stmt[:-3]

            stmt = f'{stmt} GROUP BY pitchermlbamid{groupby} ) AS pldp ON pldp.player_id = base.pitchermlbamid'

            return stmt
        
        def pitcher():
            stmt = f"LEFT OUTER JOIN ( SELECT pitchermlbamid AS player_id, SUM(sum) AS num_starts FROM pl_leaderboard_starts"
            # selections = ["pitchermlbamid as player_id", "pitcherleague", "pitcherdivision", "year_played as year",
            #                 "month_played", "half_played",  "pitcher_home_away", "SUM(gs) as games_started", "SUM(g) as games", "SUM(win) as wins", "SUM(loss) as losses", 
            #                 "SUM(cg) as complete_games", "SUM(sho) as shutouts", "SUM(qs) as quality_starts", "SUM(save) as saves", "SUM(hold) as holds", "SUM(pitches) as pitches", "SUM(outs) as outs" ]
            # # "pitchermlbamid" is just hardcoded below
            # group_by_list = [ "pitcherleague", "pitcherdivision", "year_played", "month_played", "half_played",  "pitcher_home_away"]

            # stmt = f" LEFT OUTER JOIN ( SELECT {', '.join(selections)} FROM mv_pitcher_game_stats_for_leaderboard "
            groupby = ''
            
            conditions = self.get_conditions(**kwargs)

            #get ride of hitterside and pitcherside from the inner join; probably not really necessary in the pitcher tab 
            if conditions.get('hitterside'):
                conditions.pop('hitterside')
            if conditions.get('pitcherside'):
                conditions.pop('pitcherside')
 
            if conditions:
                stmt = f"{stmt} WHERE"
                for col, val in conditions.items():
                    if (col in self.syntax_filters):
                        stmt = f"{stmt} {col} {val} AND"
                    else:
                        stmt = f"{stmt} {col} = '{val}' AND"
                        
                    groupby = f"{groupby}, {col}"
                    
                stmt = stmt[:-3]

            stmt = f"{stmt} GROUP BY pitchermlbamid{groupby}) AS start ON start.player_id = base.pitchermlbamid"
            
            return stmt

        def hitter():
            stmt = f"LEFT OUTER JOIN ( SELECT mhgs.hittermlbamid as hitter_id, sum(mhgs.g) as games, sum(mhgs.rbi) as rbi, sum(mhgs.cs) as cs, sum(mhgs.sb) as sb, sum(mhgs.runs) as runs, mhgs.year_played FROM mv_hitter_game_stats mhgs"
            groupby = ''

            #get ride of hitterside and pitcherside from the inner join; probably not really necessary in the pitcher tab 
            conditions = self.get_conditions(**kwargs)

            if conditions.get('hitterside'):
                conditions.pop('hitterside')
            if conditions.get('pitcherside'):
                conditions.pop('pitcherside')
            
            if conditions:
                stmt = f"{stmt} WHERE"
                for col, val in conditions.items():
                    if (col in self.syntax_filters):
                        stmt = f"{stmt} {col} {val} AND"
                    else:
                        stmt = f"{stmt} {col} = '{val}' AND"
                        
                    groupby = f"{groupby}, {col}"
                stmt = stmt[:-3]

            stmt = f'{stmt} GROUP BY hitter_id{groupby} ) AS totals ON totals.hitter_id = base.hittermlbamid'
            
            return stmt
 
 
        joins = {
            "pitch": pitch,
            "pitcher": pitcher,
            "hitter": hitter
        }

        return joins.get(lb, default)()
