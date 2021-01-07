#!flask/bin/python
from flask import Flask, jsonify, make_response
from flask_restful import Resource, Api, request
from flask_caching import Cache
from leaderboard import *
from datetime import datetime
from urllib.parse import urlparse
import logging
import redis

application = Flask(__name__)
api = Api(application)
# cache = Cache(application, config = {'CACHE_TYPE': 'simple'})
cache = Cache()

# cache_servers = os.environ.get('MEMCACHIER_SERVERS')
# if cache_servers == None:
#     # Fall back to simple in memory cache (development)
#     cache.init_app(application, config={'CACHE_TYPE': 'simple'})
# else:
#     cache_user = os.environ.get('MEMCACHIER_USERNAME') or ''
#     cache_pass = os.environ.get('MEMCACHIER_PASSWORD') or ''
#     cache.init_app(application,
#         config={'CACHE_TYPE': 'saslmemcached',
#                 'CACHE_MEMCACHED_SERVERS': cache_servers.split(','),
#                 'CACHE_MEMCACHED_USERNAME': cache_user,
#                 'CACHE_MEMCACHED_PASSWORD': cache_pass,
#                 'CACHE_OPTIONS': { 'behaviors': {
#                     # Faster IO
#                     'tcp_nodelay': False,
#                     # Keep connection alive
#                     'tcp_keepalive': True,
#                     # Timeout for set/get requests
#                     'connect_timeout': 2000, # ms
#                     'send_timeout': 750 * 1000, # us
#                     'receive_timeout': 750 * 1000, # us
#                     '_poll_timeout': 2000, # ms
#                     # Better failover
#                     'ketama': True,
#                     'remove_failed': 1,
#                     'retry_timeout': 2,
#                     'dead_timeout': 30}}})


redis_url = os.environ.get('REDIS_URL') or ''
cache_invalidate_hour = 10
if redis_url == '':
    cache.init_app(application, config={'CACHE_TYPE': 'simple'})
else:
    redis = urlparse(redis_url)
    cache.init_app(application,
                   config={
                       'CACHE_TYPE': 'redis',
                       'CACHE_REDIS_HOST': redis.hostname,
                       'CACHE_REDIS_PORT': redis.port,
                       'CACHE_REDIS_PASSWORD': redis.password,
                       'CACHE_REDIS_URL': redis_url,
                       'CACHE_OPTIONS': {'behaviors': {
                           # Faster IO
                           'tcp_nodelay': False,
                           # Keep connection alive
                           'tcp_keepalive': True,
                           # Timeout for set/get requests
                           'connect_timeout': 2000,  # ms
                           'send_timeout': 750 * 1000,  # us
                           'receive_timeout': 750 * 1000,  # us
                           '_poll_timeout': 2000,  # ms
                           # Better failover
                           'ketama': True,
                           'remove_failed': 1,
                           'retry_timeout': 2,
                           'dead_timeout': 30}}})


class Schedule(Resource):
    @cache.cached(timeout=300)
    def get(self, game_date):
        pl_host = os.getenv('PL_DB_HOST')
        pl_db = os.getenv('PL_DB_DATABASE', 'pitcher-list')
        pl_user = os.getenv('PL_DB_USER')
        pl_password = os.getenv('PL_DB_PW')
        db_connection = psycopg2.connect(host=pl_host, port=5432, dbname=pl_db, user=pl_user, password=pl_password)
        cursor = db_connection.cursor()
        cursor.execute("SELECT * from schedule where game_date = %s", [game_date])
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        daily_schedule = pd.DataFrame(rows, columns = colnames)
        db_connection.close()
        json_response = json.loads(daily_schedule.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class AdvancedPitcher(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            result = advanced.ArbitraryPitcher(start_date, end_date)
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = advanced.MonthlyPitcher(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = advanced.HalfPitcher(year, half)
            elif(month == "None" and half == "None"):
                result = advanced.AnnualPitcher(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class AdvancedHitter(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            result = advanced.ArbitraryHitter(start_date, end_date)
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = advanced.MonthlyHitter(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = advanced.HalfHitter(year, half)
            elif(month == "None" and half == "None"):
                result = advanced.AnnualHitter(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class AdvancedPitchType(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            result = advanced.ArbitraryPitchType(start_date, end_date)
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = advanced.MonthlyPitchType(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = advanced.HalfPitchType(year, half)
            elif(month == "None" and half == "None"):
                result = advanced.AnnualPitchType(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class ApproachPitcher(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = approach.MonthlyPitcher(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = approach.HalfPitcher(year, half)
            elif(month == "None" and half == "None"):
                result = approach.AnnualPitcher(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class ApproachHitter(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            result = approach.ArbitraryHitter(start_date, end_date)
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = approach.MonthlyHitter(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = approach.HalfHitter(year, half)
            elif(month == "None" and half == "None"):
                result = approach.AnnualHitter(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class ApproachPitchType(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = approach.MonthlyPitchType(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = approach.HalfPitchType(year, half)
            elif(month == "None" and half == "None"):
                result = approach.AnnualPitchType(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class DisciplinePitcher(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = discipline.MonthlyPitcher(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = discipline.HalfPitcher(year, half)
            elif(month == "None" and half == "None"):
                result = discipline.AnnualPitcher(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class DisciplineHitter(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = discipline.MonthlyHitter(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = discipline.HalfHitter(year, half)
            elif(month == "None" and half == "None"):
                result = discipline.AnnualHitter(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class DisciplinePitchType(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = discipline.MonthlyPitchType(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = discipline.HalfPitchType(year, half)
            elif(month == "None" and half == "None"):
                result = discipline.AnnualPitchType(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class BattedPitcher(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = batted.MonthlyPitcher(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = batted.HalfPitcher(year, half)
            elif(month == "None" and half == "None"):
                result = batted.AnnualPitcher(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class BattedHitter(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = batted.MonthlyHitter(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = batted.HalfHitter(year, half)
            elif(month == "None" and half == "None"):
                result = batted.AnnualHitter(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class BattedPitchType(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = batted.MonthlyPitchType(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = batted.HalfPitchType(year, half)
            elif(month == "None" and half == "None"):
                result = batted.AnnualPitchType(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class StandardPitcher(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = standard.MonthlyPitcher(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = standard.HalfPitcher(year, half)
            elif(month == "None" and half == "None"):
                result = standard.AnnualPitcher(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class StandardHitter(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = standard.MonthlyHitter(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = standard.HalfHitter(year, half)
            elif(month == "None" and half == "None"):
                result = standard.AnnualHitter(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class StandardPitchType(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = standard.MonthlyPitchType(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = standard.HalfPitchType(year, half)
            elif(month == "None" and half == "None"):
                result = standard.AnnualPitchType(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class OverviewPitcher(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = overview.MonthlyPitcher(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = overview.HalfPitcher(year, half)
            elif(month == "None" and half == "None"):
                result = overview.AnnualPitcher(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class OverviewHitter(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = overview.MonthlyHitter(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = overview.HalfHitter(year, half)
            elif(month == "None" and half == "None"):
                result = overview.AnnualHitter(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class OverviewPitchType(Resource):
    @cache.cached(timeout=300)
    def get(self, start_date = "None", end_date="None", year="None", month="None", half="None"):

        if(start_date != "None" and end_date != "None"):
            return {'status': 'Not Implemented'}
        elif(year != "None"):
            if(month != "None" and half == "None"):
                result = overview.MonthlyPitchType(year, month)
            elif(month == "None" and half in ["First", "Second"]):
                result = overview.HalfPitchType(year, half)
            elif(month == "None" and half == "None"):
                result = overview.AnnualPitchType(year)
            else:
                return {'status': 'Incorrect Yearly Submission'}
        else:
            return {'status': 'Incorrect Submission'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class Pitcher(Resource):
    @cache.cached(timeout=300)
    def get(self, player_id, leaderboard):

        if(leaderboard in ["Advanced", "Approach", "Discipline", "Batted", "Standard", "Overview"]):
            result = player.Pitcher(player_id, leaderboard)
        else:
            return {'status': 'Incorrect Leaderboard Submitted'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class Hitter(Resource):
    @cache.cached(timeout=300)
    def get(self, player_id, leaderboard):

        if(leaderboard in ["Advanced", "Approach", "Discipline", "Batted", "Standard", "Overview"]):
            result = player.Pitcher(player_id, leaderboard)
        else:
            return {'status': 'Incorrect Leaderboard Submitted'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class PitchType(Resource):
    @cache.cached(timeout=300)
    def get(self, player_id, leaderboard):

        if(leaderboard in ["Advanced", "Approach", "Discipline", "Batted", "Standard", "Overview"]):
            result = player.Pitcher(player_id, leaderboard)
        else:
            return {'status': 'Incorrect Leaderboard Submitted'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)


class Leaderboard_2(Resource):
    @cache.cached(timeout = cache_timeout(cache_invalidate_hour))
    def get(self, leaderboard='pitcher', handedness='NA', opponent_handedness='NA', league='NA', division='NA',
            team='NA', home_away='NA', year=datetime.now().strftime('%Y'), month='NA', half='NA', arbitrary_start='NA',
            arbitrary_end='NA'):

        raw_data = collect_leaderboard_statistics(leaderboard, handedness, opponent_handedness, league, division,
                                                   team, home_away, year, month, half, arbitrary_start, arbitrary_end)

        print("Generating subsets at {time}".format(time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        logging.debug("Generating subsets at {time}".format(time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        if leaderboard=='pitcher':
            advanced=raw_data[['player_id', 'player_name', 'player_team_abb', 'avg_velocity', 'avg_launch_speed',
                               'avg_launch_angle', 'avg_release_extension', 'avg_spin_rate', 'barrel_pct',
                               'foul_pct', 'plus_pct']]
            approach=raw_data[['player_id', 'player_name', 'player_team_abb', 'armside_pct',
                               'horizonal_middle_location_pct', 'gloveside_pct', 'high_pct',
                               'vertical_middle_location_pct', 'low_pct', 'heart_pct', 'fastball_pct',
                               'early_secondary_pct', 'late_secondary_pct', 'zone_pct', 'non_bip_strike_pct',
                               'early_bip_pct']]
            plate_discipline=raw_data[['player_id', 'player_name', 'player_team_abb', 'o_swing_pct', 'zone_pct',
                                       'swinging_strike_pct', 'called_strike_pct', 'csw_pct', 'contact_pct',
                                       'z_contact_pct', 'o_contact_pct', 'swing_pct', 'early_called_strike_pct',
                                       'late_o_swing_pct', 'f_strike_pct', 'true_f_strike_pct']]
            batted_ball=raw_data[['player_id', 'player_name', 'player_team_abb', 'groundball_pct',
                                  'linedrive_pct', 'flyball_pct', 'infield_flyball_pct', 'weak_pct',
                                  'medium_pct', 'hard_pct', 'pull_pct', 'opposite_field_pct', 'avg_launch_speed',
                                  'avg_launch_angle', 'babip_pct', 'bacon_pct']]
            overview=raw_data[['player_id', 'player_name', 'player_team_abb', 'num_ip', 'whip', 'strikeout_pct',
                               'walk_pct', 'swinging_strike_pct', 'csw_pct', 'put_away_pct', 'babip_pct',
                               'hr_flyball_pct', 'barrel_pct', 'woba']]
            standard=raw_data[['player_id', 'player_name', 'player_team_abb', 'num_pitches', 'num_hit', 'num_ip',
                               'num_hr', 'num_k', 'num_bb']]
        elif leaderboard=='hitter':
            advanced = raw_data[['player_id', 'player_name', 'player_team_abb', 'avg_launch_speed',
                                 'avg_launch_angle', 'barrel_pct', 'foul_pct', 'plus_pct', 'first_pitch_swing_pct',
                                 'early_o_contact_pct', 'late_o_contact_pct']]
            approach = raw_data[['player_id', 'player_name', 'player_team_abb', 'inside_pct',
                                 'horizonal_middle_location_pct', 'outside_pct', 'high_pct',
                                 'vertical_middle_location_pct', 'low_pct', 'heart_pct', 'fastball_pct',
                                 'early_secondary_pct', 'late_secondary_pct', 'zone_pct', 'non_bip_strike_pct',
                                 'early_bip_pct']]
            plate_discipline = raw_data[['player_id', 'player_name', 'player_team_abb', 'o_swing_pct', 'zone_pct',
                                         'swinging_strike_pct', 'called_strike_pct', 'csw_pct', 'contact_pct',
                                         'z_contact_pct', 'o_contact_pct', 'swing_pct', 'early_called_strike_pct',
                                         'late_o_swing_pct', 'f_strike_pct', 'true_f_strike_pct']]
            batted_ball = raw_data[['player_id', 'player_name', 'player_team_abb', 'groundball_pct',
                                    'linedrive_pct', 'flyball_pct', 'infield_flyball_pct', 'weak_pct',
                                    'medium_pct', 'hard_pct', 'pull_pct', 'opposite_field_pct', 'avg_launch_speed',
                                    'avg_launch_angle', 'babip_pct', 'bacon_pct']]
            overview = raw_data[['player_id', 'player_name', 'player_team_abb', 'num_pa', 'num_hr', 'batting_average',
                                 'on_base_pct', 'babip_pct', 'hr_flyball_pct', 'barrel_pct', 'swinging_strike_pct', 'woba']]
            standard = raw_data[['player_id', 'player_name', 'player_team_abb', 'num_pa', 'num_hit', 'num_1b', 'num_2b',
                                 'num_3b', 'num_hr', 'num_k', 'num_bb']]
        elif leaderboard=='pitch':
            advanced = raw_data[['player_id', 'player_name', 'player_team_abb', 'pitchtype', 'avg_velocity',
                                 'avg_launch_speed', 'avg_launch_angle', 'avg_release_extension', 'avg_spin_rate',
                                 'barrel_pct', 'avg_x_movement', 'avg_z_movement', 'plus_pct']]
            approach = raw_data[['player_id', 'player_name', 'player_team_abb', 'pitchtype', 'armside_pct',
                                 'horizonal_middle_location_pct', 'gloveside_pct', 'high_pct',
                                 'vertical_middle_location_pct', 'low_pct', 'heart_pct', 'early_pct',
                                 'behind_pct', 'late_pct', 'zone_pct', 'non_bip_strike_pct', 'early_bip_pct']]
            plate_discipline = raw_data[['player_id', 'player_name', 'player_team_abb', 'pitchtype', 'o_swing_pct',
                                         'zone_pct', 'swinging_strike_pct', 'called_strike_pct', 'csw_pct',
                                         'contact_pct','z_contact_pct', 'o_contact_pct', 'swing_pct',
                                         'early_called_strike_pct', 'late_o_swing_pct', 'f_strike_pct',
                                         'true_f_strike_pct']]
            batted_ball = raw_data[['player_id', 'player_name', 'player_team_abb', 'pitchtype', 'groundball_pct',
                                    'linedrive_pct', 'flyball_pct', 'infield_flyball_pct', 'weak_pct',
                                    'medium_pct', 'hard_pct', 'pull_pct', 'opposite_field_pct', 'avg_launch_speed',
                                    'avg_launch_angle', 'babip_pct', 'bacon_pct']]
            overview = raw_data[['player_id', 'player_name', 'player_team_abb', 'pitchtype', 'avg_velocity',
                                 'usage_pct', 'o_swing_pct', 'zone_pct', 'swinging_strike_pct', 'called_strike_pct',
                                 'csw_pct', 'put_away_pct', 'batting_average', 'woba']]
            standard = raw_data[['player_id', 'player_name', 'player_team_abb', 'pitchtype', 'num_pitches', 'num_pa',
                                 'num_hit', 'num_1b', 'num_2b', 'num_3b', 'num_hr', 'num_k', 'num_bb',
                                 'batting_average']]
        else:
            logging.error("No leaderboard of type {lb} available".format(lb=leaderboard))


        advanced_response = json.loads(advanced.to_json(orient='records', date_format='iso'))
        approach_response = json.loads(approach.to_json(orient='records', date_format='iso'))
        plate_discipline_response = json.loads(plate_discipline.to_json(orient='records', date_format='iso'))
        batted_ball_response = json.loads(batted_ball.to_json(orient='records', date_format='iso'))
        overview_response = json.loads(overview.to_json(orient='records', date_format='iso'))
        standard_response = json.loads(standard.to_json(orient='records', date_format='iso'))

        print("Subsets generated at {time}".format(time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        logging.debug("Subsets generated at {time}".format(time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        return{'advanced': advanced_response,
               'approach': approach_response,
               'plate_discipline': plate_discipline_response,
               'batted_ball': batted_ball_response,
               'overview': overview_response,
               'standard': standard_response
               }
        # return(advanced_response)


class Leaderboard_2_1(Resource):
    @cache.cached(timeout = cache_timeout(cache_invalidate_hour))
    def get(self, leaderboard='pitcher',tab='standard', handedness='NA', opponent_handedness='NA', league='NA', division='NA',
            team='NA', home_away='NA', year=datetime.now().strftime('%Y'), month='NA', half='NA', arbitrary_start='NA',
            arbitrary_end='NA'):

        result = leaderboard_collection(leaderboard, tab, handedness, opponent_handedness, league, division,
                                                   team, home_away, year, month, half, arbitrary_start, arbitrary_end)

        json_response = json.loads(result.to_json(orient='records', date_format='iso'))
        print("JSON Response {json_response}")
        logging.debug("JSON Response {json_response}")
        return (json_response)

class Players(Resource):
    @cache.cached(timeout = cache_timeout(cache_invalidate_hour))
    def get(self, player_id='NA', positions='false'):

        result = player_collection(player_id, positions)

        json_response = json.loads(result.to_json(orient='records', date_format='iso'))
        print("JSON Response {json_response}")
        logging.debug("JSON Response {json_response}")
        return (json_response)

class Player(Resource):
    @cache.cached(timeout = cache_timeout(cache_invalidate_hour))
    def get(self, player_id='NA', positions='false'):

        result = player_collection(player_id, positions)

        json_response = json.loads(result.to_json(orient='records', date_format='iso'))
        print("JSON Response {json_response}")
        logging.debug("JSON Response {json_response}")
        return (json_response)

class PlayerPositions(Resource):
    @cache.cached(timeout = cache_timeout(cache_invalidate_hour))
    def get(self, player_id='NA', positions='false'):

        result = player_collection(player_id, positions)

        json_response = json.loads(result.to_json(orient='records', date_format='iso'))
        print("JSON Response {json_response}")
        logging.debug("JSON Response {json_response}")
        return (json_response)

class PlayersPositions(Resource):
    @cache.cached(timeout = cache_timeout(cache_invalidate_hour))
    def get(self, player_id='NA', positions='false'):

        result = player_collection(player_id, positions)

        json_response = json.loads(result.to_json(orient='records', date_format='iso'))
        print("JSON Response {json_response}")
        logging.debug("JSON Response {json_response}")
        return (json_response)


class Status(Resource):
    def get(self):
        return {'status': 'available'}


class Debug(Resource):
    @cache.cached(timeout = 300)
    def get(self):
        print("The Memcache username is {memcache_user}".format(memcache_user=os.environ.get("MEMCACHIER_USERNAME")))
        print('Cache not in use')
        return {'status': "cached"}


class ClearCache(Resource):
    def get(self):
        cache.clear()
        return {'status': "cache cleared"}


# v2 Endpoints
api.add_resource(Leaderboard_2, '/v2/leaderboard/leaderboard=<string:leaderboard>&handedness=<string:handedness>&opponent_handedness=<string:opponent_handedness>&league=<string:league>&division=<string:division>&team=<string:team>&home_away=<string:home_away>&year=<string:year>&month=<string:month>&half=<string:half>&arbitrary_start=<string:arbitrary_start>&arbitrary_end=<string:arbitrary_end>')
api.add_resource(Leaderboard_2_1, '/v2_1/leaderboard/leaderboard=<string:leaderboard>&tab=<string:tab>&handedness=<string:handedness>&opponent_handedness=<string:opponent_handedness>&league=<string:league>&division=<string:division>&team=<string:team>&home_away=<string:home_away>&year=<string:year>&month=<string:month>&half=<string:half>&arbitrary_start=<string:arbitrary_start>&arbitrary_end=<string:arbitrary_end>')
api.add_resource(Players, '/v2_1/players')
api.add_resource(Player, '/v2_1/players/player_id=<string:player_id>')
api.add_resource(PlayersPositions, '/v2_1/players/positions=<string:positions>')
api.add_resource(PlayerPositions, '/v2_1/players/player_id=<string:player_id>&positions=<string:positions>')

# v1 Leaderboard Endpoints
api.add_resource(AdvancedPitcher, '/v1/Advanced/Pitcher/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(AdvancedHitter, '/v1/Advanced/Hitter/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(AdvancedPitchType, '/v1/Advanced/Pitch/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(ApproachPitcher, '/v1/Approach/Pitcher/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(ApproachHitter, '/v1/Approach/Hitter/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(ApproachPitchType, '/v1/Approach/Pitch/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(DisciplinePitcher, '/v1/Discipline/Pitcher/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(DisciplineHitter, '/v1/Discipline/Hitter/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(DisciplinePitchType, '/v1/Discipline/Pitch/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(BattedPitcher, '/v1/Batted/Pitcher/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(BattedHitter, '/v1/Batted/Hitter/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(BattedPitchType, '/v1/Batted/Pitch/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(StandardPitcher, '/v1/Standard/Pitcher/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(StandardHitter, '/v1/Standard/Hitter/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(StandardPitchType, '/v1/Standard/Pitch/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(OverviewPitcher, '/v1/Overview/Pitcher/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(OverviewHitter, '/v1/Overview/Hitter/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(OverviewPitchType, '/v1/Overview/Pitch/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')


# v1 Player Endpoints
api.add_resource(Pitcher, '/v1/Pitcher/player_id=<string:player_id>&leaderboard=<string:leaderboard>')
api.add_resource(Hitter, '/v1/Hitter/player_id=<string:player_id>&leaderboard=<string:leaderboard>')
api.add_resource(PitchType, '/v1/Pitch/player_id=<string:player_id>&leaderboard=<string:leaderboard>')


# Test Endpoints
api.add_resource(Schedule, '/v1/Schedule/<string:game_date>')
api.add_resource(Status, '/')
api.add_resource(Debug, '/Debug')
api.add_resource(ClearCache, '/Clear_Cache')

if __name__ == '__main__':
    # db_connection = get_connection()
    application.run(host='0.0.0.0', port='8080')
