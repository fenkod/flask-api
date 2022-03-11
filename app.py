#!flask/bin/python
from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from cache import init_cache
from resources import init_resource_endpoints
from calculator import init_auction_calculator
from config import base_config
import json as json
from errorhandler.errorhandler import InvalidUsage

application = Flask(__name__)
application.config

def init_api():
    application.api = Api(application)
    return application.api

with application.app_context():
    application.config.from_object(base_config)
    init_api()
    init_cache()
    init_resource_endpoints()
    # init_auction_calculator()

    @application.errorhandler(InvalidUsage)
    def handle_invalid_usage(error):
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response

if __name__ == '__main__':
    apiport = application.config.get('API_PORT')
    # db_connection = get_connection()
    application.run(host='0.0.0.0', port=apiport)
