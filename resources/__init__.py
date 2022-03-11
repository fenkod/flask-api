from flask import current_app, request
import json as json

# Endpoints for current corresponding Resources found in `/resources/`
def init_resource_endpoints():
    # Import after current app has been setup to use @current_app.cache.cached decorator
    from .player import Player
    from .roundup import Roundup
    # from .leaderboard import Leaderboard
    from .team import Team
    from .standings import Standings
    from .league import League
    from .util import Status, ClearCache
    from .leaderboard import Leaderboard
    from .auction import Auction

    # Legacy Instantiators
    from .v1 import init_v1_resource_endpoints
    from .v2 import init_v2_resource_endpoints
    from .v3 import init_v3_resource_endpoints

    # Legacy Endpoints
    init_v1_resource_endpoints()
    init_v2_resource_endpoints()
    init_v3_resource_endpoints()

    # @application.route('/v4/leaderboard', methods = ['GET'])
    # def get_leaderboard():
    #     args = request.args
    #     # Leaderboard.__init__(args)
    #     return json.dumps(args)

    # v4 resource endpoints
    v4_player_routes = [
        '/v4/player/<string:query_type>/<int:player_id>/',
        '/v4/player/<string:query_type>/<int:player_id>',
        '/v4/player/<int:player_id>/',
        '/v4/player/<int:player_id>',
        '/v4/player/<string:query_type>/',
        '/v4/player/<string:query_type>',
        '/v4/player/',
        '/v4/player'
    ]
    v4_roundup_routes = [
        '/v4/roundup/<string:player_type>/<string:day>/',
        '/v4/roundup/<string:player_type>/<string:day>',
        '/v4/roundup/<string:player_type>/',
        '/v4/roundup/<string:player_type>',
        '/v4/roundup/',
        '/v4/roundup'
    ]
    v4_leaderboard_routes = [
        # '/v4/leaderboard/leaderboard=<string:leaderboard>&tab=<string:tab>&handedness=<string:handedness>&opponent_handedness=<string:opponent_handedness>&league=<string:league>&division=<string:division>&team=<string:team>&home_away=<string:home_away>&year=<string:year>&month=<string:month>&half=<string:half>&arbitrary_start=<string:arbitrary_start>&arbitrary_end=<string:arbitrary_end>',
        '/v4/leaderboard/',
        '/v4/leaderboard'
    ]

    v4_team_routes = [
        '/v4/team/<string:query_type>/<int:team_id>/',
        '/v4/team/<string:query_type>/<int:team_id>',
        '/v4/team/<string:query_type>/',
        '/v4/team/<string:query_type>'
    ]

    v4_standings_routes = [
        '/v4/standings/<string:query_type>/<int:query_year>/',
        '/v4/standings/<string:query_type>/<int:query_year>',
        '/v4/standings/<string:query_type>/',
        '/v4/standings/<string:query_type>',
        '/v4/standings/<int:query_year>/',
        '/v4/standings/<int:query_year>',
        '/v4/standings/',
        '/v4/standings'
    ]

    v4_league_routes = [
        '/v4/league/<string:query_type>/<int:query_year>/',
        '/v4/league/<string:query_type>/<int:query_year>',
        '/v4/league/<string:query_type>/',
        '/v4/league/<string:query_type>',
        '/v4/league/<int:query_year>/',
        '/v4/league/<int:query_year>',
        '/v4/league/',
        '/v4/league'
    ]

    v4_auction_routes = [
        '/v4/auction/calculator/',
    ]

    # v4 resource endpoints
    current_app.api.add_resource(Player, *v4_player_routes, endpoint='player')
    current_app.api.add_resource(Roundup, *v4_roundup_routes, endpoint='roundup')
    current_app.api.add_resource(Leaderboard, *v4_leaderboard_routes, endpoint='leaderboard')
    current_app.api.add_resource(Standings, *v4_standings_routes, endpoint='standings')
    current_app.api.add_resource(Team, *v4_team_routes, endpoint='team')
    current_app.api.add_resource(League, *v4_league_routes, endpoint='league')
    current_app.api.add_resource(Auction, *v4_auction_routes, endpoint = 'auction')
    
    # Utility Endpoints
    current_app.api.add_resource(Status, '/')
    current_app.api.add_resource(ClearCache, '/Clear_Cache')
    