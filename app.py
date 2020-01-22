#!flask/bin/python
from flask import Flask, jsonify, make_response
from flask_restful import Resource, Api
import pandas as pd
import os
import psycopg2
import json

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

class Pitches(Resource):
    def get(self, start_date, end_date):
        pl_host = os.getenv('PL_DB_HOST')
        pl_db = 'pitcher-list'
        pl_user = os.getenv('PL_DB_USER')
        pl_password = os.getenv('PL_DB_PW')
        db_connection = psycopg2.connect(host=pl_host, port=5432, dbname=pl_db, user=pl_user, password=pl_password)
        cursor = db_connection.cursor()
        cursor.execute("SELECT * from pitches p where p.ghuid in (select s.ghuid from schedule s where s.game_date >= %s and s.game_date <= %s)", [start_date, end_date])
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        daily_schedule = pd.DataFrame(rows, columns = colnames)
        db_connection.close()
        json_response = json.loads(daily_schedule.to_json(orient='records', date_format = 'iso'))
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
                        count(*), round(avg(velo), 3), \
                        sum(case pitchresult when 'Foul' then 1 \
                        when 'Hard Foul' then 1 else 0 end), \
                        sum(case pitchresult when 'Swing Miss' then 1 \
                        when 'Called Strike' then 1 when 'Foul' then 1 \
                        when 'Hard Foul' then 1 when '-' then 1	else 0 end) \
                        from pitches where ghuid in ( select ghuid \
                        from schedule where game_date >= %s \
                        and game_date <= %s) \
                        group by pitchermlbamid, pitchername, pitchtype",
                        [start_date, end_date])
        rows = cursor.fetchall()
        colnames = ['pitchermlbamid', 'pitchername', 'pitchtype',
        'num_pitches', 'avg_velocity', 'num_foul', 'num_plus']
        adv_pt = pd.DataFrame(rows, columns = colnames)
        db_connection.close()
        adv_pt['foul_pct'] = adv_pt.apply(lambda row: round(100 * (row['num_foul'] / row['num_pitches']), 2))
        adv_pt['plus_pct'] = adv_pt.apply(lambda row: round(100 * (row['num_plus'] / row['num_pitches']), 2))
        json_response = json.loads(adv_pt.to_json(orient='records', date_format = 'iso'))
        return(json_response)

class HelloWorld(Resource):
    def get(self):
        return {'hello': 'world'}

api.add_resource(Schedule, '/v1/Schedule/<string:game_date>')
api.add_resource(Pitches, '/v1/Pitches/start_date-<string:start_date>&end_date-<string:end_date>')
api.add_resource(HelloWorld, '/')

if __name__ == '__main__':
    application.run(host='0.0.0.0', port='8080')
