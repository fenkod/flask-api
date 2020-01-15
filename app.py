#!flask/bin/python
from flask import Flask, jsonify
from flask_restful import Resource, Api
import pandas as pd
import os
import psycopg2

app = Flask(__name__)
api = Api(app)

class Schedule(Resource):
    def get(self, game_date):
        pl_host = '192.168.1.22'
        pl_db = 'pitcherlist'
        pl_user = os.getenv('PL_DB_USER')
        pl_password = os.getenv('PL_DB_PW')
        db_connection = psycopg2.connect(host=pl_host, dbname=pl_db, user=pl_user, password=pl_password)
        cursor = db_connection.cursor()
        cursor.execute("SELECT * from schedule where game_date = %s", [game_date])
        rows = cursor.fetchall()
        daily_schedule = pd.DataFrame(rows)
        db_connection.close()
        return(daily_schedule.to_json(orient='table'))

api.add_resource(Schedule, '/v1/Schedule/<string:game_date>')

if __name__ == '__main__':
    app.run(debug=True)
