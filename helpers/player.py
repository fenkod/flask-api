from resources import player

##
# Get the positions of a player.
# Caches and Returns json
##
def get_player_positions(player_id='NA'):
    # Unicode to check if numeric
    p = player.Player()
    return p.get('positions', player_id)

