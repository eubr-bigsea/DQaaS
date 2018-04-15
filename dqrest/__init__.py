from flask import Flask

app = Flask(__name__)
DATABASE_SCHEMA = 'configuration/schema.sql'
DATABASE = '/db/profiling_log.db'
