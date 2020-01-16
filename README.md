## Dependencies
-   [Python 3.5](https://python.org)
-   [Virtualenv](https://virtualenv.pypa.io)

## Setup
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
        proxy_pass http://unix:/home/api/flask-api/flask-api.sock;
        
    }
}
```
