from flask import current_app, request
import json as json
from .auction_calculator import Auction

# Endpoints for current corresponding Resources found in `/resources/`
def init_auction_calculator():
    # Import after current app has been setup to use @current_app.cache.cached decorator
    
    # v4 resource endpoints
    v4_auction_routes = [
        '/v4/auction/calculator/',
    ]
    
    current_app.api.add_resource(Auction, *v4_auction_routes, endpoint='auction')
    