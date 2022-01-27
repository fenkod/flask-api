from flask import current_app
from flask_restful import Resource
from numpy.core.records import record
from helpers import fetch_dataframe, var_dump
from cache import cache_timeout, cache_invalidate_hour
from datetime import date, datetime
import json as json
import pandas as pd

##
# This is the flask_restful Resource Class for the league averages API.
# Current Enpoint Structure:
# `/league/${query_type}/${year}`
##
class League(Resource):
    def __init__(self):
        self.query_year = None

    def get(self, query_type='NA', query_year='NA'):
        if (query_type == 'NA'):
            query_type = 'averages'
        
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
        # If running league average queries, have to run multiple queries. Need to go down a different path here
        if query_type == "averages":
            return self.fetch_averages_data(query_year)
        # Do not allow query to go through if attempting to hit the helper functions
        elif(query_type == 'startingpitcheraverages' 
            or query_type == 'reliefpitcheraverages'):
            query = self.get_query('default', query_year)
            raw = fetch_dataframe(query,query_year)
            results = self.format_results(query_type, raw)
            output = self.get_json(query_type,query_year,results)

            return output
        # Otherwise, use default query mapping
        else:
            query = self.get_query(query_type, query_year)
            raw = fetch_dataframe(query,query_year)
            results = self.format_results(query_type, raw)
            output = self.get_json(query_type,query_year,results)

            return output

    def get_query(self, query_type, query_year):
        def default():
            return f"SELECT 'query not defined' AS error, '{query_type}' AS query, {query_year} AS year;"

        def startingpitcheraverages():
            sql_query = ''

            table_select =  (f'select		mv_starting_pitcher_averages.year_played::text AS "year",'
                                        f'mv_starting_pitcher_averages.g::int,'
                                        f'mv_starting_pitcher_averages.gs::int,'
                                        f'mv_starting_pitcher_averages.w::int,'
                                        f'mv_starting_pitcher_averages.l::int,'
                                        f'mv_starting_pitcher_averages.sv::int,'
                                        f'mv_starting_pitcher_averages.bsv::int,'
                                        f'mv_starting_pitcher_averages.hld::int,'
                                        f'mv_starting_pitcher_averages.ip,'
                                        f'mv_starting_pitcher_averages.cg::int,'
                                        f'mv_starting_pitcher_averages.sho::int,'
                                        f'mv_starting_pitcher_averages.runs::int,'
                                        f'(mv_starting_pitcher_averages.runs - mv_starting_pitcher_averages.earned_runs)::int as unearned_runs,' 
                                        f'mv_starting_pitcher_averages.earned_runs::int,'
                                        f'mv_starting_pitcher_averages.era,'
                                        f'mv_starting_pitcher_averages.whip,'
                                        f'mv_starting_pitcher_averages.lob_pct,'
                                        f'mv_starting_pitcher_averages.qs::int,'
                                        f'mv_starting_pitcher_averages.x_era as "x-era",'
                                        f'mv_starting_pitcher_averages.fip,'
                                        f'mv_starting_pitcher_averages.x_fip as "x-fip",'
                                        f'mv_starting_pitcher_averages.ip_per_game as "innings-per-game",'
                                        f'mv_starting_pitcher_averages.pitches_per_game as "pitches-per-game",'
                                        f'mv_starting_pitcher_averages.hits_per_nine as "hits-per-nine",'
                                        f'mv_starting_pitcher_pitch_averages.pitchtype,'
                                        f'mv_starting_pitcher_pitch_averages.avg_velocity AS "velo_avg",'
                                        f'mv_starting_pitcher_pitch_averages.k_pct,'
                                        f'mv_starting_pitcher_pitch_averages.bb_pct,'
                                        f'mv_starting_pitcher_pitch_averages.usage_pct,'
                                        f'mv_starting_pitcher_pitch_averages.batting_average AS "batting_avg",' 
                                        f'mv_starting_pitcher_pitch_averages.o_swing_pct,'
                                        f'mv_starting_pitcher_pitch_averages.zone_pct,'
                                        f'mv_starting_pitcher_pitch_averages.swinging_strike_pct,'
                                        f'mv_starting_pitcher_pitch_averages.called_strike_pct,'
                                        f'mv_starting_pitcher_pitch_averages.csw_pct,'
                                        f'mv_starting_pitcher_pitch_averages.cswf_pct,'
                                        f'mv_starting_pitcher_pitch_averages.plus_pct,'
                                        f'mv_starting_pitcher_pitch_averages.foul_pct,'
                                        f'mv_starting_pitcher_pitch_averages.contact_pct,'
                                        f'mv_starting_pitcher_pitch_averages.o_contact_pct,'
                                        f'mv_starting_pitcher_pitch_averages.z_contact_pct,'
                                        f'mv_starting_pitcher_pitch_averages.swing_pct,'
                                        f'mv_starting_pitcher_pitch_averages.strike_pct,'
                                        f'mv_starting_pitcher_pitch_averages.early_called_strike_pct,'
                                        f'mv_starting_pitcher_pitch_averages.late_o_swing_pct,'
                                        f'mv_starting_pitcher_pitch_averages.f_strike_pct,'
                                        f'mv_starting_pitcher_pitch_averages.true_f_strike_pct,'
                                        f'mv_starting_pitcher_pitch_averages.groundball_pct,'
                                        f'mv_starting_pitcher_pitch_averages.linedrive_pct,'
                                        f'mv_starting_pitcher_pitch_averages.flyball_pct,'
                                        f'mv_starting_pitcher_pitch_averages.hr_flyball_pct,'
                                        f'mv_starting_pitcher_pitch_averages.groundball_flyball_pct,'
                                        f'mv_starting_pitcher_pitch_averages.infield_flyball_pct,'
                                        f'mv_starting_pitcher_pitch_averages.weak_pct,'
                                        f'mv_starting_pitcher_pitch_averages.medium_pct,'
                                        f'mv_starting_pitcher_pitch_averages.hard_pct,'
                                        f'mv_starting_pitcher_pitch_averages.center_pct,'
                                        f'mv_starting_pitcher_pitch_averages.pull_pct,'
                                        f'mv_starting_pitcher_pitch_averages.opposite_field_pct,'
                                        f'mv_starting_pitcher_pitch_averages.babip_pct,'
                                        f'mv_starting_pitcher_pitch_averages.bacon_pct,'
                                        f'mv_starting_pitcher_pitch_averages.armside_pct,'
                                        f'mv_starting_pitcher_pitch_averages.gloveside_pct,'
                                        f'mv_starting_pitcher_pitch_averages.vertical_middle_location_pct AS "v_mid_pct",'
                                        f'mv_starting_pitcher_pitch_averages.horizonal_middle_location_pct AS "h_mid_pct",'
                                        f'mv_starting_pitcher_pitch_averages.high_pct,'
                                        f'mv_starting_pitcher_pitch_averages.low_pct,'
                                        f'mv_starting_pitcher_pitch_averages.heart_pct,'
                                        f'mv_starting_pitcher_pitch_averages.early_pct,'
                                        f'mv_starting_pitcher_pitch_averages.behind_pct,'
                                        f'mv_starting_pitcher_pitch_averages.late_pct,'
                                        f'mv_starting_pitcher_pitch_averages.non_bip_strike_pct,'
                                        f'mv_starting_pitcher_pitch_averages.early_bip_pct,'
                                        f'mv_starting_pitcher_pitch_averages.num_pitches::int AS "pitch-count",' 
                                        f'mv_starting_pitcher_pitch_averages.num_hits::int AS "hits",' 
                                        f'mv_starting_pitcher_pitch_averages.num_bb::int AS "bb",' 
                                        f'mv_starting_pitcher_pitch_averages.num_1b::int AS "1b",' 
                                        f'mv_starting_pitcher_pitch_averages.num_2b::int AS "2b",'
                                        f'mv_starting_pitcher_pitch_averages.num_3b::int AS "3b",' 
                                        f'mv_starting_pitcher_pitch_averages.num_hr::int AS "hr",' 
                                        f'mv_starting_pitcher_pitch_averages.num_k::int AS "k",'
                                        f'mv_starting_pitcher_pitch_averages.num_pa::int AS "pa",'
                                        f'mv_starting_pitcher_pitch_averages.num_strike::int AS "strikes",' 
                                        f'mv_starting_pitcher_pitch_averages.num_ball::int AS "balls",' 
                                        f'mv_starting_pitcher_pitch_averages.num_foul::int AS "foul",' 
                                        f'mv_starting_pitcher_pitch_averages.num_ibb::int AS "ibb",' 
                                        f'mv_starting_pitcher_pitch_averages.num_hbp::int AS "hbp",' 
                                        f'mv_starting_pitcher_pitch_averages.num_wp::int AS "wp",'
                                        f'mv_starting_pitcher_pitch_averages.num_fastball AS "fastball",'
                                        f'mv_starting_pitcher_pitch_averages.num_secondary AS "secondary",'
                                        f'mv_starting_pitcher_pitch_averages.num_inside AS "inside",'
                                        f'mv_starting_pitcher_pitch_averages.num_outside AS "outside",'
                                        f'mv_starting_pitcher_pitch_averages.num_early_secondary as "early-secondary",'
                                        f'mv_starting_pitcher_pitch_averages.num_late_secondary as "late-secondary",'
                                        f'mv_starting_pitcher_pitch_averages.num_put_away as "putaway",'
                                        f'mv_starting_pitcher_pitch_averages.num_topped::int as "top-bip",'
                                        f'mv_starting_pitcher_pitch_averages.num_under::int as "under-bip",'
                                        f'mv_starting_pitcher_pitch_averages.num_flare_or_burner::int as "flare-burner-bip",'
                                        f'mv_starting_pitcher_pitch_averages.num_solid::int as "solid-bip",'
                                        f'mv_starting_pitcher_pitch_averages.num_barrel::int as "barrel-bip",'
                                        f'mv_starting_pitcher_pitch_averages.num_sweet_spot::int as "sweet-spot-bip",'
                                        f'mv_starting_pitcher_pitch_averages.num_launch_speed::int as "total-exit-velo",'
                                        f'mv_starting_pitcher_pitch_averages.num_launch_angle::int as "total-launch-angle",'
                                        f'mv_starting_pitcher_pitch_averages.num_ideal::int as "ideal-bip",'
                                        f'mv_starting_pitcher_pitch_averages.num_x_movement::int as "total-x-movement",'
                                        f'mv_starting_pitcher_pitch_averages.num_y_movement::int as "total-y-movement",'
                                        f'mv_starting_pitcher_pitch_averages.num_x_release::int as "total-x-release",'
                                        f'mv_starting_pitcher_pitch_averages.num_y_release::int as "total-y-release",'
                                        f'mv_starting_pitcher_pitch_averages.num_pitch_extension::int as "total-pitch-extension",'
                                        f'mv_starting_pitcher_pitch_averages.num_spin_rate::int as "total-spin-rate",'
                                        f'mv_starting_pitcher_pitch_averages.spin_rate_counter::int as "spin-rates-tracked",'
                                        f'mv_starting_pitcher_pitch_averages.topped_pct as "top-pct",'
                                        f'mv_starting_pitcher_pitch_averages.under_pct as "under-pct",'
                                        f'mv_starting_pitcher_pitch_averages.flare_or_burner_pct as "flare-burner-pct",'
                                        f'mv_starting_pitcher_pitch_averages.solid_pct as "solid-pct",'
                                        f'mv_starting_pitcher_pitch_averages.barrel_pct as "barrel-pct",'
                                        f'mv_starting_pitcher_pitch_averages.sweet_spot_pct as "sweet-spot-pct",'
                                        f'mv_starting_pitcher_pitch_averages.average_launch_speed as "exit-velo-avg",'
                                        f'mv_starting_pitcher_pitch_averages.average_launch_angle as "launch-angle-avg",'
                                        f'mv_starting_pitcher_pitch_averages.ideal_bbe_pct as "ideal-bbe-pct",'
                                        f'mv_starting_pitcher_pitch_averages.ideal_pa_pct as "ideal-pa-pct",'
                                        f'mv_starting_pitcher_pitch_averages.avg_x_movement as "x-movement-avg",'
                                        f'mv_starting_pitcher_pitch_averages.avg_y_movement as "y-movement-avg",'
                                        f'mv_starting_pitcher_pitch_averages.avg_x_release as "x-release-avg",'
                                        f'mv_starting_pitcher_pitch_averages.avg_y_release as "y-release-avg",'
                                        f'mv_starting_pitcher_pitch_averages.avg_pitch_extension as "pitch-extension-avg",'
                                        f'mv_starting_pitcher_pitch_averages.avg_spin_rate as "spin-rate-avg",'
                                        f'mv_starting_pitcher_pitch_averages.inside_pct as "inside-pct",'
                                        f'mv_starting_pitcher_pitch_averages.outside_pct as "outside-pct",'
                                        f'mv_starting_pitcher_pitch_averages.fastball_pct as "fastball-pct",'
                                        f'mv_starting_pitcher_pitch_averages.secondary_pct as "secondary-pct",'
                                        f'mv_starting_pitcher_pitch_averages.early_secondary_pct as "early-secondary-pct",'
                                        f'mv_starting_pitcher_pitch_averages.late_secondary_pct as "late-secondary-pct",'
                                        f'mv_starting_pitcher_pitch_averages.put_away_pct as "put-away-pct",'
                                        f'mv_starting_pitcher_pitch_averages.whiff_pct as "whiff-pct",'
                                        f'mv_starting_pitcher_pitch_averages.slug_pct as "slug-pct",'
                                        f'mv_starting_pitcher_pitch_averages.on_base_pct as "on-base-pct",'
                                        f'mv_starting_pitcher_pitch_averages.ops_pct as "ops-pct",'
                                        f'mv_starting_pitcher_pitch_averages.woba_pct as "woba-pct",'
                                        f'mv_starting_pitcher_pitch_averages.x_avg as "x-avg",'
                                        f'mv_starting_pitcher_pitch_averages.x_slug_pct as "x-slug-pct",'
                                        f'mv_starting_pitcher_pitch_averages.x_babip as "x-babip-pct",'
                                        f'mv_starting_pitcher_pitch_averages.x_woba as "x-woba-pct",'
                                        f'mv_starting_pitcher_pitch_averages.x_wobacon as "x-wobacon-pct" '
                                f'from mv_starting_pitcher_averages '
                                f'inner join mv_starting_pitcher_pitch_averages on mv_starting_pitcher_pitch_averages.year_played = mv_starting_pitcher_averages.year_played ')
            year_select = ''

            if query_year != 'NA':
                year_select = 'and mv_starting_pitcher_averages.year_played = %s '
            order_query = 'order by mv_starting_pitcher_averages.year_played'
            sql_query = table_select + year_select + order_query

            return sql_query

        def reliefpitcheraverages():
            sql_query = ''

            table_select =  (f'select		mv_relief_pitcher_averages.year_played::text AS "year",'
                                        f'mv_relief_pitcher_averages.g::int,' 
                                        f'mv_relief_pitcher_averages.gs::int,'
                                        f'mv_relief_pitcher_averages.w::int,'
                                        f'mv_relief_pitcher_averages.l::int,'
                                        f'mv_relief_pitcher_averages.sv::int,'
                                        f'mv_relief_pitcher_averages.bsv::int,'
                                        f'mv_relief_pitcher_averages.hld::int,'
                                        f'mv_relief_pitcher_averages.ip,'
                                        f'mv_relief_pitcher_averages.cg::int,'
                                        f'mv_relief_pitcher_averages.sho::int,'
                                        f'mv_relief_pitcher_averages.runs::int,'
                                        f'(mv_relief_pitcher_averages.runs - mv_relief_pitcher_averages.earned_runs)::int as unearned_runs,' 
                                        f'mv_relief_pitcher_averages.earned_runs::int,'
                                        f'mv_relief_pitcher_averages.era,'
                                        f'mv_relief_pitcher_averages.whip,'
                                        f'mv_relief_pitcher_averages.lob_pct,'
                                        f'mv_relief_pitcher_averages.qs::int,'
                                        f'mv_relief_pitcher_averages.x_era as "x-era",'
                                        f'mv_relief_pitcher_averages.fip,'
                                        f'mv_relief_pitcher_averages.x_fip as "x-fip",'
                                        f'mv_relief_pitcher_averages.ip_per_game as "innings-per-game",'
                                        f'mv_relief_pitcher_averages.pitches_per_game as "pitches-per-game",'
                                        f'mv_relief_pitcher_averages.hits_per_nine as "hits-per-nine",'
                                        f'mv_relief_pitcher_pitch_averages.pitchtype,'
                                        f'mv_relief_pitcher_pitch_averages.avg_velocity AS "velo_avg",'
                                        f'mv_relief_pitcher_pitch_averages.k_pct,'
                                        f'mv_relief_pitcher_pitch_averages.bb_pct,'
                                        f'mv_relief_pitcher_pitch_averages.usage_pct,'
                                        f'mv_relief_pitcher_pitch_averages.batting_average AS "batting_avg",' 
                                        f'mv_relief_pitcher_pitch_averages.o_swing_pct,'
                                        f'mv_relief_pitcher_pitch_averages.zone_pct,'
                                        f'mv_relief_pitcher_pitch_averages.swinging_strike_pct,'
                                        f'mv_relief_pitcher_pitch_averages.called_strike_pct,'
                                        f'mv_relief_pitcher_pitch_averages.csw_pct,'
                                        f'mv_relief_pitcher_pitch_averages.cswf_pct,'
                                        f'mv_relief_pitcher_pitch_averages.plus_pct,'
                                        f'mv_relief_pitcher_pitch_averages.foul_pct,'
                                        f'mv_relief_pitcher_pitch_averages.contact_pct,'
                                        f'mv_relief_pitcher_pitch_averages.o_contact_pct,'
                                        f'mv_relief_pitcher_pitch_averages.z_contact_pct,'
                                        f'mv_relief_pitcher_pitch_averages.swing_pct,'
                                        f'mv_relief_pitcher_pitch_averages.strike_pct,'
                                        f'mv_relief_pitcher_pitch_averages.early_called_strike_pct,'
                                        f'mv_relief_pitcher_pitch_averages.late_o_swing_pct,'
                                        f'mv_relief_pitcher_pitch_averages.f_strike_pct,'
                                        f'mv_relief_pitcher_pitch_averages.true_f_strike_pct,'
                                        f'mv_relief_pitcher_pitch_averages.groundball_pct,'
                                        f'mv_relief_pitcher_pitch_averages.linedrive_pct,'
                                        f'mv_relief_pitcher_pitch_averages.flyball_pct,'
                                        f'mv_relief_pitcher_pitch_averages.hr_flyball_pct,'
                                        f'mv_relief_pitcher_pitch_averages.groundball_flyball_pct,'
                                        f'mv_relief_pitcher_pitch_averages.infield_flyball_pct,'
                                        f'mv_relief_pitcher_pitch_averages.weak_pct,'
                                        f'mv_relief_pitcher_pitch_averages.medium_pct,'
                                        f'mv_relief_pitcher_pitch_averages.hard_pct,'
                                        f'mv_relief_pitcher_pitch_averages.center_pct,'
                                        f'mv_relief_pitcher_pitch_averages.pull_pct,'
                                        f'mv_relief_pitcher_pitch_averages.opposite_field_pct,'
                                        f'mv_relief_pitcher_pitch_averages.babip_pct,'
                                        f'mv_relief_pitcher_pitch_averages.bacon_pct,'
                                        f'mv_relief_pitcher_pitch_averages.armside_pct,'
                                        f'mv_relief_pitcher_pitch_averages.gloveside_pct,'
                                        f'mv_relief_pitcher_pitch_averages.vertical_middle_location_pct AS "v_mid_pct",'
                                        f'mv_relief_pitcher_pitch_averages.horizonal_middle_location_pct AS "h_mid_pct",'
                                        f'mv_relief_pitcher_pitch_averages.high_pct,'
                                        f'mv_relief_pitcher_pitch_averages.low_pct,'
                                        f'mv_relief_pitcher_pitch_averages.heart_pct,'
                                        f'mv_relief_pitcher_pitch_averages.early_pct,'
                                        f'mv_relief_pitcher_pitch_averages.behind_pct,'
                                        f'mv_relief_pitcher_pitch_averages.late_pct,'
                                        f'mv_relief_pitcher_pitch_averages.non_bip_strike_pct,'
                                        f'mv_relief_pitcher_pitch_averages.early_bip_pct,'
                                        f'mv_relief_pitcher_pitch_averages.num_pitches::int AS "pitch-count",' 
                                        f'mv_relief_pitcher_pitch_averages.num_hits::int AS "hits",' 
                                        f'mv_relief_pitcher_pitch_averages.num_bb::int AS "bb",' 
                                        f'mv_relief_pitcher_pitch_averages.num_1b::int AS "1b",' 
                                        f'mv_relief_pitcher_pitch_averages.num_2b::int AS "2b",'
                                        f'mv_relief_pitcher_pitch_averages.num_3b::int AS "3b",' 
                                        f'mv_relief_pitcher_pitch_averages.num_hr::int AS "hr",' 
                                        f'mv_relief_pitcher_pitch_averages.num_k::int AS "k",'
                                        f'mv_relief_pitcher_pitch_averages.num_pa::int AS "pa",'
                                        f'mv_relief_pitcher_pitch_averages.num_strike::int AS "strikes",' 
                                        f'mv_relief_pitcher_pitch_averages.num_ball::int AS "balls",' 
                                        f'mv_relief_pitcher_pitch_averages.num_foul::int AS "foul",' 
                                        f'mv_relief_pitcher_pitch_averages.num_ibb::int AS "ibb",' 
                                        f'mv_relief_pitcher_pitch_averages.num_hbp::int AS "hbp",' 
                                        f'mv_relief_pitcher_pitch_averages.num_wp::int AS "wp",'
                                        f'mv_relief_pitcher_pitch_averages.num_fastball AS "fastball",'
                                        f'mv_relief_pitcher_pitch_averages.num_secondary AS "secondary",'
                                        f'mv_relief_pitcher_pitch_averages.num_inside AS "inside",'
                                        f'mv_relief_pitcher_pitch_averages.num_outside AS "outside",'
                                        f'mv_relief_pitcher_pitch_averages.num_early_secondary as "early-secondary",'
                                        f'mv_relief_pitcher_pitch_averages.num_late_secondary as "late-secondary",'
                                        f'mv_relief_pitcher_pitch_averages.num_put_away as "putaway",'
                                        f'mv_relief_pitcher_pitch_averages.num_topped::int as "top-bip",'
                                        f'mv_relief_pitcher_pitch_averages.num_under::int as "under-bip",'
                                        f'mv_relief_pitcher_pitch_averages.num_flare_or_burner::int as "flare-burner-bip",'
                                        f'mv_relief_pitcher_pitch_averages.num_solid::int as "solid-bip",'
                                        f'mv_relief_pitcher_pitch_averages.num_barrel::int as "barrel-bip",'
                                        f'mv_relief_pitcher_pitch_averages.num_sweet_spot::int as "sweet-spot-bip",'
                                        f'mv_relief_pitcher_pitch_averages.num_launch_speed::int as "total-exit-velo",'
                                        f'mv_relief_pitcher_pitch_averages.num_launch_angle::int as "total-launch-angle",'
                                        f'mv_relief_pitcher_pitch_averages.num_ideal::int as "ideal-bip",'
                                        f'mv_relief_pitcher_pitch_averages.num_x_movement::int as "total-x-movement",'
                                        f'mv_relief_pitcher_pitch_averages.num_y_movement::int as "total-y-movement",'
                                        f'mv_relief_pitcher_pitch_averages.num_x_release::int as "total-x-release",'
                                        f'mv_relief_pitcher_pitch_averages.num_y_release::int as "total-y-release",'
                                        f'mv_relief_pitcher_pitch_averages.num_pitch_extension::int as "total-pitch-extension",'
                                        f'mv_relief_pitcher_pitch_averages.num_spin_rate::int as "total-spin-rate",'
                                        f'mv_relief_pitcher_pitch_averages.spin_rate_counter::int as "spin-rates-tracked",'
                                        f'mv_relief_pitcher_pitch_averages.topped_pct as "top-pct",'
                                        f'mv_relief_pitcher_pitch_averages.under_pct as "under-pct",'
                                        f'mv_relief_pitcher_pitch_averages.flare_or_burner_pct as "flare-burner-pct",'
                                        f'mv_relief_pitcher_pitch_averages.solid_pct as "solid-pct",'
                                        f'mv_relief_pitcher_pitch_averages.barrel_pct as "barrel-pct",'
                                        f'mv_relief_pitcher_pitch_averages.sweet_spot_pct as "sweet-spot-pct",'
                                        f'mv_relief_pitcher_pitch_averages.average_launch_speed as "exit-velo-avg",'
                                        f'mv_relief_pitcher_pitch_averages.average_launch_angle as "launch-angle-avg",'
                                        f'mv_relief_pitcher_pitch_averages.ideal_bbe_pct as "ideal-bbe-pct",'
                                        f'mv_relief_pitcher_pitch_averages.ideal_pa_pct as "ideal-pa-pct",'
                                        f'mv_relief_pitcher_pitch_averages.avg_x_movement as "x-movement-avg",'
                                        f'mv_relief_pitcher_pitch_averages.avg_y_movement as "y-movement-avg",'
                                        f'mv_relief_pitcher_pitch_averages.avg_x_release as "x-release-avg",'
                                        f'mv_relief_pitcher_pitch_averages.avg_y_release as "y-release-avg",'
                                        f'mv_relief_pitcher_pitch_averages.avg_pitch_extension as "pitch-extension-avg",'
                                        f'mv_relief_pitcher_pitch_averages.avg_spin_rate as "spin-rate-avg",'
                                        f'mv_relief_pitcher_pitch_averages.inside_pct as "inside-pct",'
                                        f'mv_relief_pitcher_pitch_averages.outside_pct as "outside-pct",'
                                        f'mv_relief_pitcher_pitch_averages.fastball_pct as "fastball-pct",'
                                        f'mv_relief_pitcher_pitch_averages.secondary_pct as "secondary-pct",'
                                        f'mv_relief_pitcher_pitch_averages.early_secondary_pct as "early-secondary-pct",'
                                        f'mv_relief_pitcher_pitch_averages.late_secondary_pct as "late-secondary-pct",'
                                        f'mv_relief_pitcher_pitch_averages.put_away_pct as "put-away-pct",'
                                        f'mv_relief_pitcher_pitch_averages.whiff_pct as "whiff-pct",'
                                        f'mv_relief_pitcher_pitch_averages.slug_pct as "slug-pct",'
                                        f'mv_relief_pitcher_pitch_averages.on_base_pct as "on-base-pct",'
                                        f'mv_relief_pitcher_pitch_averages.ops_pct as "ops-pct",'
                                        f'mv_relief_pitcher_pitch_averages.woba_pct as "woba-pct",'
                                        f'mv_relief_pitcher_pitch_averages.x_avg as "x-avg",'
                                        f'mv_relief_pitcher_pitch_averages.x_slug_pct as "x-slug-pct",'
                                        f'mv_relief_pitcher_pitch_averages.x_babip as "x-babip-pct",'
                                        f'mv_relief_pitcher_pitch_averages.x_woba as "x-woba-pct",'
                                        f'mv_relief_pitcher_pitch_averages.x_wobacon as "x-wobacon-pct" '
                                f'from mv_relief_pitcher_averages '
                                f'inner join mv_relief_pitcher_pitch_averages on mv_relief_pitcher_pitch_averages.year_played = mv_relief_pitcher_averages.year_played ')
            year_select = ''

            if query_year != 'NA':
                year_select = 'and mv_relief_pitcher_averages.year_played = %s '
            order_query = 'order by mv_relief_pitcher_averages.year_played'
            sql_query = table_select + year_select + order_query

            return sql_query

        queries = {
            "startingpitcheraverages": startingpitcheraverages,
            "reliefpitcheraverages": reliefpitcheraverages,
        }

        return queries.get(query_type, default)()

    def format_results(self, query_type, data):

        def default():
            return data

        formatting = {

        }

        return formatting.get(query_type, default)()
    
    def get_json(self, query_type, query_year, results):
        
        def default():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)
            
            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(results.to_json(orient='records', date_format='iso'))

        json_data = {
            
        }

        return json_data.get(query_type, default)()
    def fetch_averages_data(self, query_year):
        output_dict = {}

        startingPitcherAveragesQuery = self.get_query('startingpitcheraverages', query_year)
        startingPitcherAverages = fetch_dataframe(startingPitcherAveragesQuery, query_year)
        
        startingPitcherAverages.set_index(['year','pitchtype'], inplace=True)
        startingPitcherAverages.fillna(value=json.dumps(None), inplace=True)
        sp_result_dict = json.loads(startingPitcherAverages.to_json(orient='index'))

        starting_pitcher_key = 'SP'
        for keys, value in sp_result_dict.items():
            # json coversion returns tuple string
                key = eval(keys)
                year = key[0]
                if year not in output_dict:
                    output_dict[year] = {}
                if starting_pitcher_key not in output_dict[year]:
                    output_dict[year][starting_pitcher_key] = {}
                
                if "total" not in output_dict[year][starting_pitcher_key]:
                    output_dict[year][starting_pitcher_key] = { 'total': {
                        'g': value['g'],
                        'gs': value['gs'], 
                        'w': value['w'], 
                        'l': value['l'], 
                        'sv': value['sv'],
                        'bsv': value['bsv'],
                        'hld': value['hld'],
                        'ip': value['ip'],
                        'cg': value['cg'],
                        'sho': value['sho'],
                        'qs': value['qs'],
                        'runs': value['runs'],
                        'unearned_runs': value['unearned_runs'],
                        'earned_runs': value['earned_runs'],
                        'era': value['era'],
                        'whip': value['whip'],
                        'lob_pct': value['lob_pct'],
                        'x-era': value['x-era'],
                        'fip': value['fip'],
                        'x-fip': value['x-fip'],
                        'innings-per-game': value['innings-per-game'],
                        'pitches-per-game': value['pitches-per-game'],
                        'hits-per-nine': value['hits-per-nine']
                    }, 'pitches':{}}
                
                # Delete keys from value dict
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
                del value['unearned_runs']
                del value['lob_pct']
                del value['sho']
                del value['era']
                del value['whip']
                del value['x-era']
                del value['fip']
                del value['x-fip']
                del value['innings-per-game']
                del value['pitches-per-game']
                del value['hits-per-nine']

                pitch_key = key[1].upper()

                if pitch_key not in output_dict[year][starting_pitcher_key]['pitches']:
                    output_dict[year][starting_pitcher_key]['pitches'][pitch_key] = value

        reliefPitcherAveragesQuery = self.get_query('reliefpitcheraverages', query_year)
        reliefPitcherAverages = fetch_dataframe(reliefPitcherAveragesQuery, query_year)

        reliefPitcherAverages.set_index(['year','pitchtype'], inplace=True)
        reliefPitcherAverages.fillna(value=json.dumps(None), inplace=True)
        rp_result_dict = json.loads(reliefPitcherAverages.to_json(orient='index'))

        relief_pitcher_key = 'RP'
        for keys, value in rp_result_dict.items():
            # json coversion returns tuple string
                key = eval(keys)
                year = key[0]
                if year not in output_dict:
                    output_dict[year] = {}
                if relief_pitcher_key not in output_dict[year]:
                    output_dict[year][relief_pitcher_key] = {}
                
                if "total" not in output_dict[year][relief_pitcher_key]:
                    output_dict[year][relief_pitcher_key] = { 'total': {
                        'g': value['g'],
                        'gs': value['gs'], 
                        'w': value['w'], 
                        'l': value['l'], 
                        'sv': value['sv'],
                        'bsv': value['bsv'],
                        'hld': value['hld'],
                        'ip': value['ip'],
                        'cg': value['cg'],
                        'sho': value['sho'],
                        'qs': value['qs'],
                        'runs': value['runs'],
                        'unearned_runs': value['unearned_runs'],
                        'earned_runs': value['earned_runs'],
                        'era': value['era'],
                        'whip': value['whip'],
                        'lob_pct': value['lob_pct'],
                        'x-era': value['x-era'],
                        'fip': value['fip'],
                        'x-fip': value['x-fip'],
                        'innings-per-game': value['innings-per-game'],
                        'pitches-per-game': value['pitches-per-game'],
                        'hits-per-nine': value['hits-per-nine']
                    }, 'pitches':{}}
                
                # Delete keys from value dict
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
                del value['unearned_runs']
                del value['lob_pct']
                del value['sho']
                del value['era']
                del value['whip']
                del value['x-era']
                del value['fip']
                del value['x-fip']
                del value['innings-per-game']
                del value['pitches-per-game']
                del value['hits-per-nine']

                pitch_key = key[1].upper()

                if pitch_key not in output_dict[year][relief_pitcher_key]['pitches']:
                    output_dict[year][relief_pitcher_key]['pitches'][pitch_key] = value

        return output_dict
