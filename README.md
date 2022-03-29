## Dependencies
-   [Python 3.5](https://python.org)
-   [Virtualenv](https://virtualenv.pypa.io)

## Setup

### System Libraries
Prior to running this code on a Ubuntu install, you need to ensure that the appropriate system libraries are in place

```bash
sudo apt-get update
sudo apt install python-dev python3.6-dev python3.7-dev gcc libpq-dev
```

### Virtual Environment
Setup a virtual environment before starting. To do so, run:
```bash
virtualenv -p python3 flask
```

Then activate the virtual environment:
```bash
source flask.bin/activate
```

### Systemd Gunicorn Configuration (Ubuntu)

Create a service file for starting gunicorn at startup

```bash
vim /etc/systemd/system/flask-api.service
```

The contents of `flask-api.service`:

```
Description=Gunicorn instance to serve flask-api
After=network.target

[Service]
User={user}
Group=www-data
WorkingDirectory=/home/{user}/flask-api
Environment="PATH=/home/{user}/flask-api/flask/bin"
EnvironmentFile=/home/{user}/flask-api/.env
ExecStart=/home/{user}/flask-api/flask/bin/gunicorn --workers 3 --bind unix:flask-api.sock -m 007 wsgi

[Install]
WantedBy=multi-user.target 
```

The `.env` file should contain the environment variables needed to run the items in `app.py`

```
PL_DB_HOST={database_location}
PL_DB_USER={database_user}
PL_DB_PW={database_password}
```

### Nginx Configuration

Add the following server block to `/etc/nginx/nginx.conf`:

```
server {
    listen 80;
    server_name 34.195.125.234;
    
    location / { 
        proxy_read_timeout 300s;
        proxy_connect_timeout 75s;
        proxy_set_header Host $http_host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Pror $scheme;
        proxy_pass http://unix:/home/{user}/flask-api/flask-api.sock;
        
    }
}
```


## Endpoints

The URL structure for the v2 of the leaderboard API is http://api.dfccnp.com/v2_1/leaderboard/leaderboard=pitch&tab=overview&handedness=NA&opponent_handedness=NA&league=NA&division=NA&team=NA&home_away=NA&year=2019&month=5&half=NA&arbitrary_start=NA&arbitrary_end=NA


- `leaderboard`: one of pitch, pitcher, or hitter
- `tab`: one of overview, standard, advanced, approach, plate_discipline, batted_ball
- `handedness`: one of R, L, or NA
- `opponent_handedness`: one of R, L, or NA
- `league`: one of AL, NL, or NA
- `division`: one of East, Central, West, or NA
- `team`: the three letter abbreviation for a team or NA
- `home_away`: one of home, away, or NA
- `year`: four digit year or NA if using arbitrary start and end
- `month`: 1-12, NA if using arbitrary start and end or not filtering on month
- `half`: one of First, Second, or  NA if using arbitrary start and end or not filtering on half
- `arbitrary_start`: ISO date format if submitting a custom date range or NA if using the year+
- `arbitrary_end`: ISO date format if submitting a custom date range or NA if using the year+


NA is always used when not submitting an explicit value for a field.

# Auction Calculator

## Consuming

- staging: https://pitcherlist-api-staging.herokuapp.com/v4/auction/calculator
- production: https://api.dfccnp.com/v4/auction/calculator

parameters:
|value|values|required|default|description|
|-|-|-|-|-|
|points|`p`, `c`|no|`p`|How the auction calculator should intake relevant categories. `c` = categories; `p` = points|
|league|`MLB`, `AL`, `NL`|no|`MLB`||
|teams|4 <= teams <= 24|no|12|Number of teams in the league.|
|pos|see description|no|`1,1,1,1,1,3,0,2,1,1,2,2,5,10`|Number of positions rosterable in the following order: C, 1B, 2B, 3B, SS, OF, DH, UTIL, MI, CI, SP, RP, P, BN|
|mp|1<=mp<=100|no|20|Max roster size (but this isn't used)|
|msp|0<=msp<=20|no|20|Max starting pitchers|
|mrp|0<=mrp<=20|no|20|Max relief pitchers|
|mb|1<=mb<=1000000|no|1|Minimum bid during the draft|
|split|0.1<=split<=0.9|no|0.7|Percent of league that is allocated to hitters|
|h|If points = p, a string of 13 numbers correlated to the categories listed in the descriptions. <br><hr> If points = c, a string of categories that should be applied for the calculation.<br>Valid categores: 'AVG', 'RBI', 'R', 'SB', 'HR', 'OBP', 'SLG', 'OPS', 'H', 'SO', 'S', 'D', 'T', 'TB', 'BB', 'RBI+R', 'xBH', 'SB-CS', 'wOBA'|no|If points = p: `0,0,1,2,3,4,-1,1,0,1,0,1,1` <br><hr> If points = `c`: `AVG,RBI,R,SB,HR` |If points = `p`, this correlates to the number of points associated with each unit in the following order: 'PA', 'H', 'S', 'D', 'T', 'HR', 'SO', 'BB', 'HBP', 'SB', 'CS', 'R', 'RBI'.  <br><hr> If points = `c`, these are the relevant categories. Example: `p=HR,RBI,SB,AVG,R`
|p|If points = p, a string of 13 numbers correlated to the categories listed in the descriptions. <br><hr> If points = c, a string of categories that should be applied for the calculation. Valid categories: 'W', 'SV', 'ERA', 'WHIP', 'SO', 'AVG', 'K/9', 'BB/9', 'K/BB', 'IP', 'QS', 'HR', 'HLD', 'SV+HLD'|no|If points = `p` : `1,0,5,-5,5,0,1,-1,0,-2,0,-1,0` <br><hr> If points = c: `W,SO,ERA,WHIP,SV` |If points = `p`, this correlates to the number of points associated with each unit in the following order: 'Out', 'QS', 'W', 'L', 'SV', 'HLD', 'K', 'BB', 'HBP', 'ER', 'R', 'H', 'HR'. <br><hr> If points = `c`, these are the relevant 
categories. Example: `p=SO,WHIP,QS,ERA,SV`

       batting_categories = ['avg', 'rbi', 'r', 'sb', 'hr', 'obp', 'slg', 'ops', 'h', 'so', 's', 'd', 't', 'tb', 'bb', 'RBI+R', 'xBH', 'SB-CS', 'woba']
        pitching_categories = ['w', 'sv', 'era', 'whip', 'so', 'avg', 'K/9', 'BB/9', 'K/BB', 'ip', 'qs', 'hr', 'hld', 'SV+HLD']