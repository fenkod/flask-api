#!flask/bin/python
from flask import Flask
from flask_restful import Resource, Api
from cache import init_cache
from resources import init_resource_endpoints

application = Flask(__name__)

# Set API in our current_app context
def init_api():
    application.api = Api(application)
    return application.api

with application.app_context():
    init_api()
    init_cache()
    init_resource_endpoints()

if __name__ == '__main__':
    # db_connection = get_connection()
    application.run(host='0.0.0.0', port='8080')
