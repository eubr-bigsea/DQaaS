from flask import g
from flask_restful import Resource, Api
from dqrest import app, DATABASE_SCHEMA
from dqrest.resources.assessment import Assessment
from dqrest.resources.profiling import Profiling
from dqrest.resources.config import Config
from dqrest.resources.requests import Requests
from dqrest.resources.error_log import ErrorLog
from dqrest.common.db import get_db
from dqrest.common.hdfs import create_config_dir, create_log_dir

api = Api(app)

@app.teardown_appcontext
def close_connection(exception):
    """Called on teardown to close DB instances"""
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@app.cli.command('init')
def init():
    """Initialize the db creating tables."""
    db = get_db()
    with app.open_resource(DATABASE_SCHEMA, mode='r') as f:
        db.cursor().executescript(f.read())
    create_log_dir()
    create_config_dir()
    print("Database initialized and dir created.")


api.add_resource(Profiling, '/profiling', '/profiling/<string:uuid>')
api.add_resource(Assessment, '/assessment', '/assessment/<string:uuid>')
api.add_resource(Config, '/config_file/<string:file_id>')
api.add_resource(Requests, '/get_all')
api.add_resource(ErrorLog, '/error_log/<string:uuid>')
