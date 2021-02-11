from flask import current_app

# Endpoints for current corresponding Resources found in `/resources/`
def init_resource_endpoints():
    # Import after current app has been setup to use @current_app.cache.cached decorator
    from .leaderboard import Leaderboard
    from .player import Player
    from .util import Status, ClearCache

    # Legacy Instantiators
    from .v1 import init_v1_resource_endpoints
    from .v2 import init_v2_resource_endpoints

    # Legacy Endpoints
    init_v1_resource_endpoints()
    init_v2_resource_endpoints()

    # v3 resource endpoints
    # Current endpoints: (repertoire,positions,stats,gamelogs)
    v3_player_routes = [
        '/v3/player/<string:query_type>/<int:player_id>/',
        '/v3/player/<int:player_id>/',
        '/v3/player/<string:query_type>/',
        '/v3/player/'
    ]
    v3_leaderboard_routes = [
        '/v3/leaderboard/<string:query_type>/',
        '/v3/leaderboard/'
    ]
    current_app.api.add_resource(Leaderboard, *v3_leaderboard_routes, endpoint='leaderboard')
    current_app.api.add_resource(Player, *v3_player_routes, endpoint='player')

    # Utility Endpoints
    current_app.api.add_resource(Status, '/')
    current_app.api.add_resource(ClearCache, '/Clear_Cache')