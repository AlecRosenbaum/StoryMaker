#!/usr/bin/env bash
# FLASK_DEBUG=1 FLASK_APP=server.py flask run
# gunicorn -k gevent -w 1 server:app
python3 server.py