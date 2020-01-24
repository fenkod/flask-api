#!flask/bin/python
from flask import Flask, jsonify, make_response
from flask_restful import Resource, Api
import pandas as pd
import os
import psycopg2
import json
from functools import reduce

application = Flask(__name__)
api = Api(application)

class Schedule(Resource):
    def get(self, game_date):
        pl_host = os.getenv('PL_DB_HOST')
        pl_db = 'pitcher-list'
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
    def get(self, start_date, end_date):
        pl_host = os.getenv('PL_DB_HOST')
        pl_db = 'pitcher-list'
        pl_user = os.getenv('PL_DB_USER')
        pl_password = os.getenv('PL_DB_PW')
        db_connection = psycopg2.connect(host=pl_host, port=5432, dbname=pl_db, user=pl_user, password=pl_password)
        cursor = db_connection.cursor()
        cursor.execute("select pitchermlbamid, pitchername, count(*), avg(velo), \
                        sum(case pitchresult when 'Foul' then 1 \
                        when 'Hard Foul' then 1 else 0 end), \
                        sum(case when pitchresult = 'Swing Miss' then 1 \
                        when pitchresult = 'Called Strike' then 1 \
                        when pitchresult = 'Foul' AND (strikes = 0 OR strikes = 1) then 1 \
                        when pitchresult = 'Hard Foul' AND (strikes = 0 OR strikes = 1) then 1 \
                        when pitchresult = '-' then 1 else 0 end) \
                        from pitches where ghuid in ( select ghuid \
                        from schedule where game_date >= %s \
                        and game_date <= %s) and pitchtype <> 'IN'\
                        and ghuid in (select ghuid from game_detail \
                        where postseason = false) \
                        group by pitchermlbamid, pitchername",
                        [start_date, end_date])
        rows = cursor.fetchall()
        colnames = ['pitchermlbamid', 'pitchername', 'num_pitches',
        'avg_velocity', 'num_foul', 'num_plus']
        ie_adv_pt = pd.DataFrame(rows, columns = colnames)
        db_connection.close()

        bs_db = 'baseballsavant'
        db_connection = psycopg2.connect(host=pl_host, port=5432, dbname=bs_db, user=pl_user, password=pl_password)
        cursor = db_connection.cursor()
        cursor.execute("select pl.mlb_id, count(*), \
                       avg(p.launch_speed), avg(p.launch_angle), \
                       avg(p.release_extension) , avg(p.spin_rate) , \
                       avg(p.release_position_x - p.plate_x) , \
                       avg(p.release_position_z - p.plate_z) , \
                       sum(case m.launch_speed_angle_code when 6 then 1 \
                       else 0 end) from pitches p join matchups m \
                       on p.matchup_id = m.id join players pl \
                       on m.pitcher_id = pl.id where m.game_id in \
                       (select id from games where game_date >= %s \
                       and game_date <= %s) \
                       group by pl.mlb_id", [start_date, end_date])
        rows = cursor.fetchall()
        colnames = ['pitchermlbamid', 'num_pitches_bs', 'avg_ev','avg_la', 'avg_ext',
        'avg_spin', 'avg_x_mov', 'avg_z_mov', 'num_barrel']
        bs_adv_pt = pd.DataFrame(rows, columns = colnames)
        db_connection.close()

        leaderboard = [ie_adv_pt, bs_adv_pt]
        adv_pt = reduce(lambda left,right: pd.merge(left,right,on=['pitchermlbamid'],how='outer'), leaderboard)

        adv_pt['foul_pct'] = adv_pt.apply(lambda row: 100 * (int(row['num_foul']) / int(row['num_pitches'])), axis = 1)
        adv_pt['plus_pct'] = adv_pt.apply(lambda row: 100 * (int(row['num_plus']) / int(row['num_pitches'])), axis = 1)
        adv_pt['barrel_pct'] = adv_pt.apply(lambda row: 100 * (int(row['num_barrel']) / int(row['num_pitches'])), axis = 1)
        json_response = json.loads(adv_pt.to_json(orient='records', date_format = 'iso'))
        return(json_response)

class AdvancedPitchType(Resource):
    def get(self, start_date, end_date):
        pl_host = os.getenv('PL_DB_HOST')
        pl_db = 'pitcher-list'
        pl_user = os.getenv('PL_DB_USER')
        pl_password = os.getenv('PL_DB_PW')
        db_connection = psycopg2.connect(host=pl_host, port=5432, dbname=pl_db, user=pl_user, password=pl_password)
        cursor = db_connection.cursor()
        cursor.execute("select pitchermlbamid, pitchername, pitchtype, \
                        count(*), avg(velo), \
                        sum(case pitchresult when 'Foul' then 1 \
                        when 'Hard Foul' then 1 else 0 end), \
                        sum(case when pitchresult = 'Swing Miss' then 1 \
                        when pitchresult = 'Called Strike' then 1 \
                        when pitchresult = 'Foul' AND (strikes = 0 OR strikes = 1) then 1 \
                        when pitchresult = 'Hard Foul' AND (strikes = 0 OR strikes = 1) then 1 \
                        when pitchresult = '-' then 1 else 0 end) \
                        from pitches where ghuid in ( select ghuid \
                        from schedule where game_date >= %s \
                        and game_date <= %s) and pitchtype != 'IN' \
                        group by pitchermlbamid, pitchername, pitchtype",
                        [start_date, end_date])
        rows = cursor.fetchall()
        colnames = ['pitchermlbamid', 'pitchername', 'pitchtype',
        'num_pitches', 'avg_velocity', 'num_foul', 'num_plus']
        adv_pt = pd.DataFrame(rows, columns = colnames)
        db_connection.close()

        #bs_db = 'baseballsavant'
        #db_connection = psycopg2.connect(host=pl_host, port=5432, dbname=bs_db, user=pl_user, password=pl_password)
        #cursor = db_connection.cursor()
        #cursor.execute("select pl.mlb_id, p.pitch_type_abbreviation, count(*), \
        #                avg(p.launch_speed), avg(p.launch_angle), \
        #                avg(p.release_extension) , avg(p.spin_rate) , \
        #                avg(p.release_position_x - p.plate_x) , \
        #                avg(p.release_position_z - p.plate_z) , \
        #                sum(case m.launch_speed_angle_code when 6 then 1 \
        #                else 0 end) from pitches p join matchups m \
        #                on p.matchup_id = m.id join players pl \
        #                on m.pitcher_id = pl.id where m.game_id in \
        #                (select id from games where game_date >= %s \
        #                and game_date <= %s) \
        #                group by pl.mlb_id, p.pitch_type_abbreviation",
        #                [start_date, end_date])
        # rows = cursor.fetchall()
        # colnames = ['mlb_id', 'pitch_type', 'num_pitches_bs', 'avg_ev',
        # 'avg_la', 'avg_ext', 'avg_spin', 'avg_x_mov', 'avg_z_mov', 'is_barrel']
        # bs_adv_pt = pd.DataFrame(rows, colnames)
        # db_connection.close()
        #
        # pitch_map = {
        #     "Unknown": "UNK",
        #     "SL": "SL",
        #     "FF": "FA",
        #     "CH": "CH",
        #     "CU": "CU",
        #     "FT": "FA",
        #     "SI": "SI",
        #     "FC": "FC",
        #     "FS": "FS",
        #     "KC": "CU",
        #     "EP": "EP",
        #     "KN": "KN",
        #     "FO": "FS",
        # }

        adv_pt['foul_pct'] = adv_pt.apply(lambda row: 100 * (int(row['num_foul']) / int(row['num_pitches'])), axis = 1)
        adv_pt['plus_pct'] = adv_pt.apply(lambda row: 100 * (int(row['num_plus']) / int(row['num_pitches'])), axis = 1)
        json_response = json.loads(adv_pt.to_json(orient='records', date_format = 'iso'))
        return(json_response)

class Status(Resource):
    def get(self):
        return {'status': 'available'}

api.add_resource(Schedule, '/v1/Schedule/<string:game_date>')
api.add_resource(AdvancedPitcher, '/v1/Advanced/Pitcher/start_date=<string:start_date>&end_date=<string:end_date>')
api.add_resource(AdvancedPitchType, '/v1/Advanced/Pitch/start_date=<string:start_date>&end_date=<string:end_date>')
api.add_resource(Status, '/')

if __name__ == '__main__':
    application.run(host='0.0.0.0', port='8080')
