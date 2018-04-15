import sqlite3

from dqrest import DATABASE
from flask import g


"""
DB MANAGEMENT
"""


def get_db():
    """DQ factory

    Returns an instance of the db.
    """
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE, isolation_level=None)
    return db


def mark_job_as_done(pid):
    """Mark as done the request identified by <pid>.

    Args:
        pid: The unique identifier of the request (uuid4).
    """
    cur = get_db().cursor()
    cur.execute(
        'UPDATE requests SET done = 1, completed_timestamp = CURRENT_TIMESTAMP WHERE uuid = ? ;', (str(pid),))
    cur.close()


def mark_job_as_error(pid):
    """Signal that the request identified by <pid> exited with an error code.

    Args:
        pid: The unique identifier of the request (uuid4).
    """
    cur = get_db().cursor()
    cur.execute(
        'UPDATE requests SET done = 1, completed_timestamp = CURRENT_TIMESTAMP, error = 1 WHERE uuid = ?', (str(pid),))
    cur.close()
