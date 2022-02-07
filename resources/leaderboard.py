from signal import raise_signal
from flask import current_app, Flask, request
from flask_restful import Resource
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
    woba_list = ['num_ab', 'num_bb', 'num_ibb', 'num_hbp', 'num_sacrifice_fly', 'num_1b', 'num_2b', 'num_3b', 'num_hr']
    leaderboard_kwargs = {
        "leaderboard" : fields.Str(required=False, missing="pitcher", validate=validate.OneOf(["pitcher", "pitch", "hitter"])),
        "tab" : fields.Str(required=False, missing="overview", validate=validate.OneOf(["overview", "standard", "statcast", "batted_ball", "batted_ball_2", "approach"])),
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
                "overview": [
                            'avg_velocity', 'usage_pct', 'o_swing_pct', 'zone_pct', 'swinging_strike_pct',
                            'called_strike_pct', 'csw_pct', 'put_away_pct', 'batting_average', 'num_pitches', 'strike_pct',
                            'plus_pct','groundball_pct', 'flyball_pct', 'woba', 'babip_pct', 'hr_flyball_pct', 'x_avg', 'x_woba',
                            'hard_pct', 'avg_spin_rate', 'num_pitches'],
                "statcast": ['x_avg', 'x_woba', 'x_babip', 'x_wobacon', 'hard_pct', 'barrel_pct', 'ideal_bbe_pct', 'ideal_pa_pct', 
                            'average_launch_speed', 'average_fly_ball_launch_speed', 'average_launch_angle', 'avg_x_movement', 'avg_y_movement',
                            'avg_x_release', 'avg_y_release', 'avg_pitch_extension', 'avg_spin_rate', 'num_pitches'],
                "batted_ball": ['groundball_pct', 'linedrive_pct', 'flyball_pct', 'infield_flyball_pct', 'weak_pct',
                                'medium_pct', 'hard_pct', 'pull_pct', 'opposite_field_pct', 'bacon_pct', 'num_pitches', 'foul_pct',
                                'hr_flyball_pct', 'center_pct', 'topped_pct', 'under_pct', 'flare_or_burner_pct', 'solid_pct', 'sweet_spot_pct'],
                "batted_ball_2": ['num_pitches', 'hard_pct', 'barrel_pct', 'ideal_bbe_pct', 'ideal_pa_pct', 'batting_average', 'woba', 
                            'babip_pct', 'bacon_pct', 'x_avg', 'x_woba', 'x_babip', 'x_wobacon', 'average_launch_speed', 'average_fly_ball_launch_speed',
                            'average_launch_angle'],
                "plate_discipline": ['o_swing_pct', 'zone_pct', 'swinging_strike_pct', 'called_strike_pct', 'csw_pct', 'strike_pct', 'plus_pct',
                                    'whiff_pct', 'swing_pct', 'contact_pct', 'z_contact_pct', 'o_contact_pct',
                                    'early_called_strike_pct', 'late_o_swing_pct', 'f_strike_pct', 'true_f_strike_pct', 'num_pitches'],
                "approach": ['armside_pct','horizonal_middle_location_pct', 'gloveside_pct', 'high_pct',
                            'vertical_middle_location_pct', 'low_pct', 'heart_pct', 'inside_pct', 'outside_pct', 'early_pct','behind_pct', 'late_pct',
                            'zone_pct', 'non_bip_strike_pct', 'early_bip_pct', 'put_away_pct', 'num_pitches'],                
                "standard": ['num_pitches', 'num_pa', 'num_hit', 'num_1b', 'num_2b', 'num_3b', 'num_hr', 'num_k', 'num_bb', 'num_hbp', 'num_wp', 
                            'num_strikes', 'num_balls', 'batting_average', 'slug_pct', 'woba']
            },
            "pitcher": {
                "overview": [#'games','wins','losses','complete_games','shutouts','quality_starts','saves','holds','pitches',
                            'num_ip', 'era', 'whip', 'strikeout_pct', 'walk_pct', 'swinging_strike_pct', 'csw_pct',
                            'put_away_pct', 'babip_pct', 'hr_flyball_pct', 'plus_pct', 'wins', 'losses', 'x_era', 'num_hits_per_nine',
                            'fip', 'x_fip','x_babip', 'hard_pct', 'groundball_pct','lob_pct', 'swinging_strike_pct', 'csw_pct'],
                "standard": ['num_pitches', 'num_hit', 'num_ip', 'era', 'num_hr', 'num_k', 'num_bb', 'ipg', 'ppg', 'wins', 'losses',
                            'saves', 'holds', 'cg', 'qs', 'sho', 'num_hbp', 'num_wp', 'num_runs','num_hit',
                            'num_1b', 'num_2b', 'num_3b', 'num_hr', 'num_ibb', 'num_bb', 'num_k', 'num_earned_runs'],
                "statcast": ['strikeout_pct', 'walk_pct', 'batting_average', 'slug_pct', 'on_base_pct', 'woba', 'babip_pct', 'bacon_pct',
                            'x_avg', 'x_slug_pct', 'x_woba', 'x_babip', 'x_wobacon', 'x_era', 'average_launch_speed', 'average_launch_angle', 'ops_pct'],
                "batted_ball": ['groundball_pct', 'linedrive_pct', 'flyball_pct', 'infield_flyball_pct', 'weak_pct',
                                'medium_pct', 'hard_pct', 'pull_pct', 'opposite_field_pct', 'bacon_pct', 'num_pitches', 'foul_pct',
                                'hr_flyball_pct', 'center_pct', 'topped_pct', 'under_pct', 'flare_or_burner_pct', 'solid_pct', 'sweet_spot_pct'],
                "batted_ball_2": ['num_pitches', 'hard_pct', 'barrel_pct', 'ideal_bbe_pct', 'ideal_pa_pct', 'batting_average', 'woba', 
                            'babip_pct', 'bacon_pct', 'x_avg', 'x_woba', 'x_babip', 'x_wobacon', 'average_launch_speed', 'average_fly_ball_launch_speed',
                            'average_launch_angle'],
                "plate_discipline": ['o_swing_pct', 'zone_pct', 'swinging_strike_pct', 'called_strike_pct', 'csw_pct', 'plus_pct',
                                    'contact_pct', 'z_contact_pct', 'o_contact_pct', 'swing_pct', 'strike_pct', 'early_called_strike_pct',
                                    'late_o_swing_pct', 'f_strike_pct', 'true_f_strike_pct', 'num_ip'],
                "approach": ['armside_pct','horizonal_middle_location_pct', 'gloveside_pct', 'high_pct',
                            'vertical_middle_location_pct', 'low_pct', 'heart_pct', 'inside_pct', 'outside_pct', 'early_pct','behind_pct', 'late_pct',
                            'zone_pct', 'non_bip_strike_pct', 'early_bip_pct', 'put_away_pct', 'num_pitches']                
            },
            "hitter": {
                "overview": ['num_games_played', 'num_runs_scored', 'num_rbi', 'num_sb', 'num_cs', 'num_pa', 'num_hr', 'num_runs', 'num_rbi',  'batting_average', 'on_base_pct', 'babip_pct', 'hr_flyball_pct',
                            'swinging_strike_pct', 'woba', 'strikeout_pct', 'walk_pct',  'slug_pct', 'x_avg', 'x_woba', 
                            'flyball_pct','groundball_pct', 'ideal_pa_pct', 'hard_pct', 'barrel_pct', 'csw_pct'],
                "standard": ['num_pa', 'num_hit', 'num_1b', 'num_2b', 'num_3b', 'num_hr', 'num_k', 'num_bb', 
                            'num_sb', 'num_cs', 'batting_average', 'on_base_pct', 'woba', 'num_hard_bip', 'num_barrel','num_hbp'],
                "statcast":['strikeout_pct', 'walk_pct', 'batting_average', 'slug_pct' , 'on_base_pct', 'woba', 'babip_pct', 'bacon_pct', 'x_avg', 
                            'x_slug_pct', 'x_woba', 'x_babip', 'x_wobacon', 'max_launch_speed', 'average_launch_speed',
                            'average_fly_ball_launch_speed', 'average_launch_angle', 'ops_pct'], 
                "batted_ball": ['groundball_pct', 'linedrive_pct', 'flyball_pct', 'infield_flyball_pct', 'pull_pct', 'opposite_field_pct',
                                'babip_pct', 'bacon_pct', 'num_pa','center_pct', 'foul_pct'],
                "batted_ball_2": ['weak_pct','topped_pct', 'under_pct', 'flare_or_burner_pct', 'solid_pct', 'barrel_pct', 'hard_pct',
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
            # This section is game stats and cannot be queried directly from the pl_leaderboard_daily view
            "num_earned_runs": 0,
            "sho": 0,
            "cg": 0,
            "qs": 0,
            "holds": 0,
            "saves": 0,
            "ppg": 0,
            "ipg": 0,
            "lob_pct": 0,
            "num_hits_per_nine": 0,
            "x_fip": 0,
            "fip": 0,
            "losses": 0,
            "wins": 0,
            "era": "ROUND(COALESCE(SUM(num_runs::numeric), 0::bigint)::numeric / NULLIF(SUM(num_outs::numeric) / 3.0, 0::numeric) * 9.0, 2)",
            "x_era": 0,
            # columns for hitter overview that is not at a pitch level
            "num_games_played": "totals.games",
            "num_rbi": "totals.rbi",
            "num_runs_scored": "totals.runs",
            "num_sb": "totals.sb",
            "num_cs": "totals.cs",
            # Columns broken up by pitch
            "ops_pct": "ROUND((SUM(num_hit) + SUM(num_bb) + SUM(num_hbp)) / NULLIF((SUM(num_ab) + SUM(num_bb) + SUM(num_sacrifice) + SUM(num_hbp)), 0), 3) + round((sum(num_1b) + 2::numeric * sum(num_2b) + 3::numeric * sum(num_3b) + 4::numeric * sum(num_hr)) / NULLIF(sum(num_ab), 0::numeric), 3)",
            "max_launch_speed": "round(max(max_launch_speed), 1)",
            "x_slug_pct": "round((sum(num_xsingle) + 2::numeric * sum(num_xdouble) + 3::numeric * sum(num_xtriple) + 4::numeric * sum(num_xhomerun)) / NULLIF(sum(num_ab), 0::numeric), 3)",
            "slug_pct": "round((sum(num_1b) + 2::numeric * sum(num_2b) + 3::numeric * sum(num_3b) + 4::numeric * sum(num_hr)) / NULLIF(sum(num_ab), 0::numeric), 3)",
            "num_strikes": "SUM(num_strikes)",
            "num_balls": "SUM(num_balls)",
            "num_wp": "SUM(num_wp)",
            "put_away_pct": "round(100::numeric * (sum(num_put_away) / NULLIF(sum(num_late), 0::numeric)), 1)",
            "whiff_pct": "round(100::numeric * (sum(num_whiff) / NULLIF(sum(num_swing), 0::numeric)), 1)",
            "topped_pct": "round(100::numeric * (sum(num_topped) / NULLIF(sum(num_batted_ball_event), 0::numeric)), 1)",
            "under_pct": "round(100::numeric * (sum(num_under) / NULLIF(sum(num_batted_ball_event), 0::numeric)), 1)",
            "flare_or_burner_pct": "round(100::numeric * (sum(num_flare_or_burner) / NULLIF(sum(num_batted_ball_event), 0::numeric)), 1)",
            "solid_pct": "round(100::numeric * (sum(num_solid) / NULLIF(sum(num_batted_ball_event), 0::numeric)), 1)",
            "sweet_spot_pct": "round(100::numeric * (sum(num_sweet_spot) / NULLIF(sum(num_batted_ball_event), 0::numeric)), 1)",
            "bacon_pct": "round(sum(num_hit) / NULLIF(sum(num_ab) - sum(num_k) + sum(num_sacrifice), 0::numeric), 3)",
            "x_babip": "round((sum(num_xhit) - sum(num_xhomerun)) / NULLIF(sum(num_ab) - sum(num_xhomerun) - sum(num_k) + sum(num_sacrifice), 0::numeric), 3)",
            "x_wobacon": "round((max(woba_bb) * sum(num_bb - num_ibb) + max(woba_hbp) * sum(num_hbp) + max(woba_single) * sum(num_xsingle) + max(woba_double) * sum(num_xdouble) + max(woba_triple) * sum(num_xtriple) + max(woba_home_run) * sum(num_xhomerun)) / NULLIF(sum(num_ab) + sum(num_bb) - sum(num_ibb) + sum(num_sacrifice_fly) + sum(num_hbp), 0::numeric), 4)",
            "ideal_bbe_pct": "round(100::numeric * (sum(num_ideal) / NULLIF(sum(num_batted_ball_event), 0::numeric)), 1)",
            "ideal_pa_pct":  "round(100::numeric * (sum(num_ideal) / NULLIF(sum(num_pa), 0::numeric)), 1)",
            "average_launch_speed": "round(sum(num_launch_speed) / NULLIF(sum(num_batted_ball_event), 0::numeric), 1)",
            "average_fly_ball_launch_speed": "round(sum(num_fly_ball_launch_speed) / NULLIF(sum(num_fly_ball), 0::numeric), 1)",
            "average_launch_angle": "round(sum(num_launch_angle) / NULLIF(sum(num_batted_ball_event), 0::numeric), 1)",
            "avg_x_movement": "round(sum(num_x_movement) / NULLIF(sum(location_counter), 0::numeric), 1)",
            "avg_y_movement": "round(sum(num_y_movement) / NULLIF(sum(location_counter), 0::numeric), 1)",
            "avg_x_release": "round(sum(num_x_release) / NULLIF(sum(location_counter), 0::numeric), 1)",
            "avg_y_release": "round(sum(num_y_release) / NULLIF(sum(location_counter), 0::numeric), 1)",
            "avg_pitch_extension": "round(sum(num_pitch_extension) / NULLIF(sum(location_counter), 0::numeric), 1)",
            "avg_spin_rate": "round(sum(num_spin_rate) / NULLIF(sum(spin_rate_counter), 0::numeric), 1)",
            "woba": "round((max(woba_bb) * sum(num_bb - num_ibb) + max(woba_hbp) * sum(num_hbp) + max(woba_single) * sum(num_1b) + max(woba_double) * sum(num_2b) + max(woba_triple) * sum(num_3b) + max(woba_home_run) * sum(num_hr)) / NULLIF(sum(num_ab) + sum(num_bb) - sum(num_ibb) + sum(num_sacrifice_fly) + sum(num_hbp), 0), 4)",
            "x_woba": "round((max(woba_bb) * sum(num_bb - num_ibb) + max(woba_hbp) * sum(num_hbp) + max(woba_single) * sum(num_xsingle) + max(woba_double) * sum(num_xdouble) + max(woba_triple) * sum(num_xtriple) + max(woba_home_run) * sum(num_xhomerun)) / NULLIF(sum(num_ab) + sum(num_bb) - sum(num_ibb) + sum(num_sacrifice_fly) + sum(num_hbp), 0), 4)",
            "x_avg": "ROUND(SUM(num_xhit) / NULLIF(SUM(num_ab), 0), 3)",
            "num_pitches": "SUM(base.num_pitches)",
            "avg_velocity": "ROUND(SUM(total_velo) / NULLIF(SUM(num_velo), 0), 1)",
            "barrel_pct": "ROUND(100 * (SUM(num_barrel) / NULLIF(SUM(num_batted_ball_event), 0)), 1)",
            "foul_pct": "ROUND(100 * (SUM(num_foul) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "plus_pct": "ROUND(100 * (SUM(num_plus_pitch) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "first_pitch_swing_pct": "ROUND(100 * (SUM(num_first_pitch_swing) / NULLIF(SUM(num_ab), 0)), 1)",
            "early_o_contact_pct": "ROUND(100 * (SUM(num_early_o_contact) / NULLIF(SUM(num_o_contact), 0)), 1)",
            "late_o_contact_pct": "ROUND(100 * (SUM(num_late_o_contact) / NULLIF(SUM(num_o_contact), 0)), 1)",
            #"avg_launch_speed": "ROUND((SUM(total_launch_speed) / NULLIF(SUM(num_launch_speed), 0))::DECIMAL, 1)",
            #"avg_launch_angle": "ROUND((SUM(total_launch_angle) / NULLIF(SUM(num_launch_angle), 0))::DECIMAL, 1)",
            #"avg_release_extension": "ROUND((SUM(total_release_extension) / NULLIF(SUM(num_release_extension), 0))::DECIMAL, 1)",
            #"avg_spin_rate": "round(sum(num_spin_rate) / NULLIF(sum(spin_rate_counter), 0)::DECIMAL, 1)",
            #"avg_x_movement": "ROUND((SUM(total_x_movement) / NULLIF(SUM(num_x_movement), 0))::DECIMAL, 1)",
            #"avg_z_movement": "ROUND((SUM(total_z_movement) / NULLIF(SUM(num_z_movement), 0))::DECIMAL, 1)",
            "armside_pct": "ROUND(100 * (SUM(num_armside) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "babip_pct": "ROUND(((SUM(num_hit) - SUM(num_hr)) / NULLIF((sum(num_ab) - SUM(num_hr) - SUM(num_k) + SUM(num_sacrifice)), 0)), 3)",
            "gloveside_pct": "ROUND(100 * (SUM(num_gloveside) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "inside_pct": "ROUND(100 * (SUM(num_inside) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "outside_pct": "ROUND(100 * (SUM(num_outside) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "high_pct": "ROUND(100 * (SUM(num_high) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "horizonal_middle_location_pct": "ROUND(100 * (SUM(num_horizontal_middle) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "vertical_middle_location_pct": "ROUND(100 * (SUM(num_middle) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "low_pct": "ROUND(100 * (SUM(num_low) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "heart_pct": "ROUND(100 * (SUM(num_heart) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "early_pct": "ROUND(100 * (SUM(num_early) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "behind_pct": "ROUND(100 * (SUM(num_behind) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "late_pct": "ROUND(100 * (SUM(num_late) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "zone_pct": "ROUND(100 * (SUM(num_zone) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "non_bip_strike_pct": "ROUND(100 * (SUM(num_non_bip_strike) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "early_bip_pct": "ROUND(100 * (SUM(num_early_bip) / NULLIF(SUM(base.num_early), 0)), 1)",
            "groundball_pct": "ROUND(100 * (SUM(num_ground_ball) / NULLIF(SUM(num_batted_ball_event), 0)), 1)",
            "linedrive_pct": "ROUND(100 * (SUM(num_line_drive) / NULLIF(SUM(num_batted_ball_event), 0)), 1)",
            "flyball_pct": "ROUND(100 * (SUM(num_fly_ball) / NULLIF(SUM(num_batted_ball_event), 0)), 1)",
            "infield_flyball_pct": "ROUND(100 * (SUM(num_if_fly_ball) / NULLIF(SUM(num_batted_ball_event), 0)), 1)",
            "weak_pct": "ROUND(100 * (SUM(num_weak_bip) / NULLIF(SUM(num_batted_ball_event), 0)), 1)",
            "medium_pct": "ROUND(100 * (SUM(num_medium_bip) / NULLIF(SUM(num_batted_ball_event), 0)), 1)",
            "hard_pct": "ROUND(100 * (SUM(num_hard_bip) / NULLIF(SUM(num_pa), 0)), 1)",
            "pull_pct": "ROUND(100 * (SUM(num_pulled_bip) / NULLIF(SUM(num_batted_ball_event), 0)), 1)",
            "center_pct": "ROUND(100 * (SUM(num_center_bip) / NULLIF(SUM(num_batted_ball_event), 0)), 1)",
            "opposite_field_pct": "ROUND(100 * (SUM(num_opposite_bip) / NULLIF(SUM(num_batted_ball_event), 0)), 1)",
            "swing_pct": "ROUND(100 * (SUM(num_swing) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "o_swing_pct": "ROUND(100 * (SUM(num_o_swing) / NULLIF(SUM(num_swing), 0)), 1)",
            "z_swing_pct": "ROUND(100 * (SUM(num_z_swing) / NULLIF(SUM(num_swing), 0)), 1)",
            "contact_pct": "ROUND(100 * (SUM(num_contact) / NULLIF(SUM(num_swing), 0)), 1)",
            "o_contact_pct": "ROUND(100 * (SUM(num_o_contact) / NULLIF(SUM(num_o_swing), 0)), 1)",
            "z_contact_pct": "ROUND(100 * (SUM(num_z_contact) / NULLIF(SUM(num_z_swing), 0)), 1)",
            "swinging_strike_pct": "ROUND(100 * (SUM(num_whiff) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "called_strike_pct": "ROUND(100 * (SUM(num_called_strike) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "csw_pct": "ROUND(100 * (SUM(num_called_strike_plus_whiff) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "strike_pct": "ROUND(100 * (SUM(num_strikes) / NULLIF(SUM(base.num_pitches), 0)), 1)",
            "early_called_strike_pct": "ROUND(100 * (SUM(num_early_called_strike) / NULLIF(SUM(num_early), 0)), 1)",
            "late_o_swing_pct": "ROUND(100 * (SUM(num_late_o_swing) / NULLIF(SUM(num_late), 0)), 1)",
            "f_strike_pct": "ROUND(100 * (SUM(num_first_pitch_strike) / NULLIF(SUM(num_pa), 0)), 1)",
            "true_f_strike_pct": "ROUND(100 * (SUM(num_true_first_pitch_strike) / NULLIF(SUM(num_pa), 0)), 1)",
            "put_away_pct": "ROUND(100 * (SUM(num_put_away) / NULLIF(SUM(num_late), 0)), 1)",
            "batting_average": "ROUND(SUM(num_hit) / NULLIF(SUM(num_ab), 0), 3)",
            "strikeout_pct": "ROUND(100 * (SUM(num_k) / NULLIF(SUM(num_pa), 0)), 1)",
            "walk_pct": "ROUND(100 * (SUM(num_bb) / NULLIF(SUM(num_pa), 0)), 1)",
            "hr_flyball_pct": "ROUND(100 * (SUM(num_hr) / NULLIF((SUM(num_fly_ball) + SUM(num_sacrifice)), 0)), 3)",
            "on_base_pct": "ROUND((SUM(num_hit) + SUM(num_bb) + SUM(num_hbp)) / NULLIF((SUM(num_ab) + SUM(num_bb) + SUM(num_sacrifice) + SUM(num_hbp)), 0), 3)",
            "whip": "ROUND((SUM(num_hit) + SUM(num_bb)) / NULLIF(SUM(num_outs) / 3, 0), 2)",
            "num_ip": "ROUND(NULLIF(SUM(num_outs) / 3, 0), 1)",
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
        
    @use_kwargs(leaderboard_kwargs)
    def get(self, **kwargs):

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

        return self.fetch_result(kwargs.get('leaderboard'), **kwargs)
                    
    
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
                result = self.fetch_data(query_type, **query_args)
                # Set Cache expiration to 5 mins
                current_app.cache.set(cache_key, result, 300)

        return result

    def fetch_data(self, query_type, **query_args):
        query = self.get_query(query_type, **query_args)
        var_dump(query)
        cursor_list = self.build_cursor_execute_list(query_type, **query_args)
        raw = fetch_dataframe(query, cursor_list)
        results = self.format_results(query_type, raw)
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
        table = self.get_table()
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

        queries = {
            "pitch": pitch,
            "pitcher": pitcher,
            "hitter": hitter
        }

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
                'player_team': 'pitcherteam',
                'player_team_abb': 'pitcherteam_abb',
                'player_side': 'pitcherside',
                'player_side_against': 'hitterside',
                'player_league': 'pitcherleague',
                'player_division': 'pitcherdivision',
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
                'player_team': 'pitcherteam',
                'player_team_abb': 'pitcherteam_abb',
                'player_side': 'pitcherside',
                'player_side_against': 'hitterside',
                'player_league': 'pitcherleague',
                'player_division': 'pitcherdivision',
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
                'player_team': 'hitterteam',
                'player_team_abb': 'hitterteam_abb',
                'player_side': 'hitterside',
                'player_side_against': 'pitcherside',
                'player_league': 'hitterleague',
                'player_division': 'hitterdivision',
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
        
        cols = {
            "pitch": pitch,
            "pitcher": pitcher,
            "hitter": hitter
        }

        return cols.get(leaderboard, pitcher)()
    
    def get_table(self):
        # TODO: UPDATE View and remove version from name
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
            groupby_fields = ["pitchermlbamid", "pitchername", "pitcherteam", "pitcherteam_abb", "pitchtype", "pldp.num_pitches", "start.num_starts"]
            for field in groupby_fields:
                groupby.append(field)

            return groupby

        def pitcher():
            groupby_fields = ["base.pitchermlbamid", "pitchername", "pitcherteam", "pitcherteam_abb", "start.num_starts"]
            for field in groupby_fields:
                groupby.append(field)

            return groupby

        def hitter():

            groupby_fields = ["hittermlbamid", "hittername", "hitterteam", "hitterteam_abb"]

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
        
        groups = {
            "pitch": pitch,
            "pitcher": pitcher,
            "hitter": hitter
        }

        return groups.get(kwargs.get('leaderboard'), pitcher)()

    def get_joins(self, lb, **kwargs):
        def default():
            return None

        def pitch():
            stmt = self.get_joins('pitcher',**kwargs)
            table = self.get_table()

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
            # stmt = f"LEFT OUTER JOIN ( SELECT pitchermlbamid AS player_id, SUM(sum) AS num_starts FROM pl_leaderboard_starts"
            stmt = f"INNER JOIN ( SELECT pitchermlbamid AS player_id, SUM(sum) AS num_starts FROM pl_leaderboard_starts"
            groupby = ''
            
            conditions = self.get_conditions(**kwargs)

            #get ride of hitterside and pitcherside from the inner join; probably not really necessary in the pitcher tab 
            if conditions.get('hitterside'):
                conditions.pop('hitterside')
            if conditions.get('pitcherside'):
                conditions.pop('pitcherside')    
            if conditions.get('pitcherleague'):
                conditions.pop('pitcherleague')
            if conditions.get('pitcherdivision'):
                conditions.pop('pitcherdivision')            
            if conditions:
                stmt = f"{stmt} WHERE"
                for col, val in conditions.items():
                    if (col in self.syntax_filters):
                        stmt = f"{stmt} {col} {val} AND"
                    else:
                        stmt = f"{stmt} {col} = '{val}' AND"
                        
                    groupby = f"{groupby}, {col}"
                stmt = stmt[:-3]

            stmt = f'{stmt} GROUP BY pitchermlbamid{groupby} ) AS start ON start.player_id = base.pitchermlbamid'
            
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
