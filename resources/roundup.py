import re
from statistics import mode
from flask import current_app
from flask_restful import Resource
from sqlalchemy import false
from helpers import fetch_dataframe, date_validate, var_dump
import json as json
from datetime import date, datetime
from webargs import fields, validate
from webargs.flaskparser import use_kwargs, parser, abort
import os # For retrieving credentials
import http.client # For Sport Radar API
from cache import cache_timeout, cache_invalidate_hour

##
# This is the flask_restful Resource Class for the SP Roundup and Batterbox API.
# It accepts both the batterbox and roundup endpoints 
# Current Enpoint Structure:
# `/roundup/${player_type}/${day}` - Hitters and Pitchers
# @param ${player_type}: ('hitter'|'pitcher')
# @param ${day}: ([0-9]2/[0-9]2/[0-9]4|'latest')
##
class Roundup(Resource):
    roundup_kwargs = {
        "type": fields.Str(required=False, missing="pitcher", validate=validate.OneOf(["pitcher","hitter"])),
        "day": fields.Str(required=False, missing="NA"), #ISO date format
        "bypass_cache": fields.Str(required=False, missing="NA", validate=validate.OneOf(["true","false"])),
        "mode": fields.Str(required=False, missing="original", validate=validate.OneOf(["original","advanced"])),
    }
    def __init__(self, *args, **kwargs):
        self.bypass_cache = False
        

    @use_kwargs(roundup_kwargs)
    def get(self, **kwargs):
        # Pitcher or Hitter     
        player_type = kwargs['type']
        # Date
        if ( kwargs['day'] != 'NA'):
            input_date = datetime.strptime(kwargs['day'], '%Y-%m-%d')
        else:
            input_date = date.today()
        # Mode
        mode = kwargs['mode']
        # Bypass cache
        if(kwargs['bypass_cache'] != 'NA'):
            self.bypass_cache = bool(kwargs['bypass_cache'])

        # Get the latest day
        self.day = input_date
        self.player_type = player_type
        self.mode = mode

        return self.fetch_result(player_type, input_date, mode)

    def fetch_result(self, player_type, input_date, mode):
        data = self.fetch_data(player_type, input_date, mode)
        results = self.get_json(player_type, input_date, mode, data)

        return results
    
    def fetch_data(self, player_type, input_date, mode):

        # Caching wrapper for fetch_data
        games = []
        endpoints = SportRadarEndpoints()
        if player_type == 'pitcher':
            year = input_date.strftime('%Y') 
            month = input_date.strftime('%m')
            day = input_date.strftime('%d')

            # Get daily summary data from Sport Radar
            daily_summary = endpoints.daily_summary_endpoint(year, month, day)
            # Get list of games for the day
            daily_games = daily_summary['league']['games']
            # Iterrate through games
            for row in daily_games: #[:3]:
                game = row['game']
                game_id = game['id']
                home_team = game['home']
                away_team = game['away']
                needs_home_data = True
                needs_away_data = True
                home_pitcher_cache_key = self.BuildCacheKey(game_id, home_team['id'])
                away_pitcher_cache_key = self.BuildCacheKey(game_id, away_team['id'])      
                # If cache is not bypassed, see if both results are available in the cache 
                if(self.bypass_cache == False):
                    home_pitcher_cache_result = current_app.cache.get(home_pitcher_cache_key)
                    if home_pitcher_cache_result is not None:
                        games.append(home_pitcher_cache_result)
                        needs_home_data = False
                    away_pitcher_cache_result = current_app.cache.get(away_pitcher_cache_key)
                    if away_pitcher_cache_result is not None:
                        games.append(away_pitcher_cache_result)
                        needs_away_data = False
                # If both results are cached, continue to next game
                if(needs_home_data == False and needs_away_data == False):
                    continue
                print(f"Building Game: {away_team['abbr']} @ {home_team['abbr']}")
                # Gather other basic game information to build a game model
                scheduled_date = game['scheduled']
                game_status = game['status']

                game_model = {
                    'game-date': scheduled_date,
                    'reference': game['reference'],
                    'status': game_status
                }

                if 'venue' in game:
                    venue = game['venue']
                    #del venue['market']
                    #del venue['capacity']
                    #del venue['surface']
                    #del venue['address']
                    #del venue['zip']
                    #del venue['country']
                    #del venue['field_orientation']
                    #del venue['location']
                    #game_model['venue'] = venue

                if 'weather' in game:
                    weather = game['weather']
                    game_model['weather'] = weather
                if 'final' in game:
                    final = game['final']
                    game_model['final'] = final
                if 'outcome' in game:
                    outcome = game['outcome']
                    #del outcome['type']
                    #del outcome['hitter']['id']
                    #del outcome['pitcher']['id']
                    #del outcome['pitcher']['pitch_speed']
                    #del outcome['pitcher']['pitch_type']
                    #del outcome['pitcher']['pitch_zone']
                    #del outcome['pitcher']['pitch_x']
                    #del outcome['pitcher']['pitch_y']
                    game_model['outcome'] = outcome

                # If game is one of these statuses, it has not started. Figure out who the projected
                # pitchers are/were and return a basic model with game status
                if(game_status in ('scheduled', 'canceled', 'postponed', 'if-necessary')):
                    home_pitcher_model = self.BuildScheduledGame("HOME", home_team, away_team, game_model)
                    games.append(home_pitcher_model)
                    away_pitcher_model = self.BuildScheduledGame("AWAY", away_team, home_team, game_model)
                    games.append(away_pitcher_model)
                # Game has started. Get details
                else:
                    # Gather hit play-by-play endpoint, build model, set cache and return data
                    play_by_play_data = endpoints.play_by_play_endpoint(game_id)
                    if(needs_home_data):
                        home_pitcher_model = self.BuildInProgressGame("HOME", home_team, away_team, game_model, play_by_play_data)
                        games.append(home_pitcher_model)
                        current_app.cache.set(home_pitcher_cache_key, home_pitcher_model, cache_timeout(cache_invalidate_hour()))
                    if(needs_away_data):
                        away_pitcher_model = self.BuildInProgressGame("AWAY", away_team, home_team, game_model, play_by_play_data)
                        games.append(away_pitcher_model)
                        current_app.cache.set(away_pitcher_cache_key, away_pitcher_model, cache_timeout(cache_invalidate_hour()))
            result = {'date': input_date.strftime("%a %m/%d/%Y"), 'games': games}
            return result
        else:
            return {}
    def BuildCacheKey(self, game_id, team_id):
        cache_key_resource_type = self.__class__.__name__
        return f'{cache_key_resource_type}-{game_id}-{team_id}'

    def BuildScheduledGame(self, home_away, team, opponent, game_model):
        pitcher = None
        if 'probable_pitcher' in team:
            pitcher = team['probable_pitcher']

        model = {
            # Legacy Data
            'player_id': 0,
            'team': team['abbr'],
            'playername': None,
            'park': home_away,
            'opponent': opponent['abbr'],
            'game_pk': game_model['reference'],
            'stats': None,
            'ingame': None,
            # New Data
            'pitcher': None,   
            'game': game_model,
            'pitches': None,
            'gamestarted': False
        }

        if pitcher is not None:
            model['playername'] = f"{pitcher['preferred_name']} {pitcher['last_name']}"
            model['pitcher'] = pitcher
    
        return model

    def BuildInProgressGame(self, home_away, team, opponent, game_model, play_by_play_data):
        pitcher = team['starting_pitcher']
        
        game_stats = team['statistics']['pitching']['starters']

        innings = play_by_play_data['game']['innings']
    
        pitches = []
        for inning in innings:
            if inning['number'] > 0:
                for half in inning['halfs']:
                    h = half['half']
                    for e in half['events']:
                        if list(e.keys())[0] == 'at_bat':
                            atBat = e['at_bat']
                            if len(atBat['events']) != 0:
                                l = len(atBat['events'])
                                for atBatEvent in atBat['events']:
                                    if "type" in atBatEvent.keys():
                                        if atBatEvent['type'] == 'pitch':
                                            if(atBatEvent.get('pitcher').get('id') == pitcher['id']):
                                                pitch = atBatEvent
                                                pitch['inning'] = inning['number']
                                                pitch['inning-half'] = h
                                                if(atBatEvent.get('flags').get('is_ab_over')):
                                                    pitch['at-bat-description'] = atBat['description']   
                                                pitches.append(pitch)                                                                             

        still_in_game = True
        lineups = team['lineup']
        for lineuprecord in lineups:
            if lineuprecord['position'] == 1 and lineuprecord['inning'] > 0:
                still_in_game = False
                break
        model = {
            # Legacy Data
            'player_id': 0,
            'team': team['abbr'],
            'playername': f"{pitcher['preferred_name']} {pitcher['last_name']}",
            'park': home_away,
            'opponent': opponent['abbr'],
            'game_pk': game_model['reference'],
            'stats': game_stats,
            'ingame': still_in_game,
            # New Data
            'pitcher': pitcher,   
            'game': game_model,
            'pitches': pitches,
            'gamestarted': True
        }
    
        return model

    def get_json(self, player_type, day, mode, results):
        
        def default():
            # Ensure we have valid data for NaN entries using json.dumps of Python None object
            results.fillna(value=json.dumps(None), inplace=True)
            
            # Allow date formatting to_json instead of to_dict. Convert back to dict with json.loads
            return json.loads(results.to_json(orient='records', date_format='iso'))
        
        def roundup():
            if(self.player_type == 'pitcher'):
                if(self.mode == 'original'):
                    #results.fillna(value=0, inplace=True)
                    #records = json.loads(results.to_json(orient='records'))
                    
                    output = {
                        "date": results["date"]
                    }
                    startedgames = []
                    scheduledgames = []
                    # Keep game data on top level and move all stats to its own object. Allows us to use for pitchers and hitters without needing to change code.
                    if "games" in results:
                        for game in results["games"]:
                            pitcher = {
                                "player_id": game["player_id"], 
                                "team": game["team"],
                                "playername": game["playername"],
                                "park": game["park"],
                                "opponent": game["opponent"],
                                "game_pk": game["game_pk"]
                            }
                            
                            # Line Status
                            line_status = ""
                            if not game['gamestarted']:
                                line_status = "Scheduled"
                            elif 'final' in game['game']:
                                line_status = "Final"
                            elif game['ingame']:
                                line_status = "Still in Game"
                            elif not game['ingame']:
                                line_status = "Out of Game"
                            pitcher['line_status'] = line_status

                            if game["stats"] != None:
                                gamestats = game["stats"]

                                # Decision
                                if gamestats['games']['win'] == 1:
                                    pitcher['decision'] = "W"
                                elif gamestats['games']['loss'] == 1:
                                    pitcher['decision'] = "L"
                                else:
                                    pitcher['decision'] = "ND"
                                # Pitch Related Data
                                pitches = game["pitches"]
                                calledstrikes = 0
                                whiffs = 0
                                for pitch in pitches:
                                    pitchoutcome = pitch["outcome_id"]
                                    if "KL" in pitchoutcome:
                                        calledstrikes = calledstrikes + 1
                                    if "KS" in pitchoutcome or pitchoutcome == "kFT" :
                                        whiffs = whiffs + 1
                                # Pitcher stats object
                                pitcher["stats"] = {
                                    "ip": gamestats["ip_2"],
                                    "er": gamestats["runs"]["earned"],
                                    "hits": gamestats["onbase"]["h"],
                                    "k": gamestats["outs"]["ktotal"],
                                    "bb": gamestats["onbase"]["bb"] + gamestats["onbase"]["ibb"],
                                    "pitch-count": gamestats["pitch_count"],
                                    "whiff": whiffs,
                                    "csw_pct": round(100 * (calledstrikes + whiffs) / gamestats["pitch_count"], 2)
                                }
                                startedgames.append(pitcher)
                            else:
                                pitcher["stats"] = None
                                scheduledgames.append(pitcher) 

                            
                    # Sort by ER, then IP (desc)
                    startedgames.sort(key=lambda x: (x['stats']['er'], -x['stats']['ip']))
                    output["games"] = startedgames + scheduledgames
                    return output
                elif(self.mode == 'advanced'):
                    return results
            else:
                return results

        json_data = {
            "hitter": roundup,
            "pitcher": roundup
        }

        return json_data.get(player_type, default)()


class SportRadarEndpoints:
    def __init__(self):
       self.access_level = 'tracking'
       self.version = 'v7'
       self.file_format = 'json'
       self.api_key = os.getenv('SPORTRADAR_STATCAST_API_KEY')

    def retrieve_sport_radar_data(self, endpoint):
        """
        This function retrieves the daily boxscore information as defined in the 
        daily boxscore endpoint.
        
        inputs
        endpoint = sport radar endpoint api (string)
        
        outputs
        json of endpoint data
        """ 

        # Connect to Sport Radar API and retrieve data
        conn = http.client.HTTPSConnection("api.sportradar.us")   
        conn.request("GET", endpoint)
        res = conn.getresponse()
        data = res.read()
        data = data.decode("utf-8")

        return data

    def daily_summary_endpoint(self, year=None,month=None,day=None):

        """
        This function retrieves the daily summary.
        
        inputs
        year, month, day = date of daily summary, default is todays date (string)
        month and day data must be in 'MM' or 'DD' format, i.e. '05' instead of 5
        
        outputs
        dictionary of endpoint data
        """ 

        today = date.today()

        if not year:
            year = today.year
        if not month:
            month = today.month
        if not day:
            day = today.day

        # Build endpoint
        endpoint = f'/mlb/{self.access_level}/{self.version}/en/games/{year}/{month}/{day}/summary.{self.file_format}?api_key={self.api_key}'

        # Connect to Sport Radar API and retrieve data
        data = self.retrieve_sport_radar_data(endpoint)
        
        # Convert json data to python dictionary
        data = json.loads(data) 

        return data

    def play_by_play_endpoint(self, game_id):

        """
        Detailed real-time information on every pitch and game event.

        inputs
        game_id = sport radar game id

        outputs
        dictionary of endpoint data
        """ 

        # Build endpoint
        endpoint = f'/mlb/{self.access_level}/{self.version}/en/games/{game_id}/pbp.{self.file_format}?api_key={self.api_key}'

        # Connect to Sport Radar API and retrieve data
        data = self.retrieve_sport_radar_data(endpoint)
        
        # Convert json data to python dictionary
        data = json.loads(data) 

        return data