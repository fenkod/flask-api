#!flask/bin/python
from flask import Flask, jsonify, make_response
from flask_restful import Resource, Api
import pandas as pd
import os
import psycopg2
import json
from functools import reduce
from leaderboard import *

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

class StandardHitter(Resource):
    def get(self, start_date, end_date):
        result = standard.Hitter(start_date = start_date, end_date = end_date)
        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)

class ApproachHitter(Resource):
    def get(self, start_date, end_date):
        result = approach.Hitter(start_date, end_date)
        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)

class Status(Resource):
    def get(self):
        return {'status': 'available'}

api.add_resource(Schedule, '/v1/Schedule/<string:game_date>')
api.add_resource(AdvancedPitcher, '/v1/Advanced/Pitcher/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(AdvancedHitter, '/v1/Advanced/Hitter/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(AdvancedPitchType, '/v1/Advanced/Pitch/start_date=<string:start_date>&end_date=<string:end_date>&year=<string:year>&month=<string:month>&half=<string:half>')
api.add_resource(StandardHitter, '/v1/Standard/Hitter/start_date=<string:start_date>&end_date=<string:end_date>')
api.add_resource(ApproachHitter, '/v1/Approach/Hitter/start_date=<string:start_date>&end_date=<string:end_date>')
api.add_resource(Status, '/')

if __name__ == '__main__':
    application.run(host='0.0.0.0', port='8080')
