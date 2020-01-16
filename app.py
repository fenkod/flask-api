#!flask/bin/python
from flask import Flask, jsonify, make_response
from flask_restful import Resource, Api
import pandas as pd
import os
import psycopg2

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
        daily_schedule = pd.DataFrame(rows)
        db_connection.close()
        return(daily_schedule.to_json(orient='records', date_format = 'iso'))

class HelloWorld(Resource):
    def get(self):
        return {'hello': 'world'}

api.add_resource(Schedule, '/v1/Schedule/<string:game_date>')
api.add_resource(HelloWorld, '/')

if __name__ == '__main__':
    application.run(host='0.0.0.0', port='8080')
