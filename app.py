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

class ApproachPitcher(Resource):
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
    def get(self, player_id, leaderboard):

        if(leaderboard in ["Advanced", "Approach", "Discipline", "Batted", "Standard", "Overview"]):
            result = player.Pitcher(player_id, leaderboard)
        else:
            return {'status': 'Incorrect Leadboard Sumbmitted'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)

class Hitter(Resource):
    def get(self, player_id, leaderboard):

        if(leaderboard in ["Advanced", "Approach", "Discipline", "Batted", "Standard", "Overview"]):
            result = player.Pitcher(player_id, leaderboard)
        else:
            return {'status': 'Incorrect Leadboard Sumbmitted'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)

class PitchType(Resource):
    def get(self, player_id, leaderboard):

        if(leaderboard in ["Advanced", "Approach", "Discipline", "Batted", "Standard", "Overview"]):
            result = player.Pitcher(player_id, leaderboard)
        else:
            return {'status': 'Incorrect Leadboard Sumbmitted'}

        json_response = json.loads(result.to_json(orient='records', date_format = 'iso'))
        return(json_response)

class Status(Resource):
    def get(self):
        return {'status': 'available'}

api.add_resource(Schedule, '/v1/Schedule/<string:game_date>')
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
api.add_resource(Pitcher, '/v1/Pitcher/player_id=<string:player_id>&leaderboard=<string:leaderboard>')
api.add_resource(Hitter, '/v1/Hitter/player_id=<string:player_id>&leaderboard=<string:leaderboard>')
api.add_resource(PitchType, '/v1/Pitch/player_id=<string:player_id>&leaderboard=<string:leaderboard>')
api.add_resource(Status, '/')

if __name__ == '__main__':
    application.run(host='0.0.0.0', port='8080')
