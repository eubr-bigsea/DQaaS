from flask import request, render_template, make_response
from flask_restful import Resource
from dqrest.common.db import get_db
from dqrest.common.error import gen_error_response
from dqrest.common.hdfs import cat_file, HDFSLOGDIR


class ErrorLog(Resource):

    def get(self, uuid):
        cur = get_db().cursor()
        rv = cur.execute(
            'SELECT error, type FROM requests WHERE uuid = ?', (uuid,)).fetchall()
        if(len(rv) == 0):
            return gen_error_response(404, 'No request with that uuid')
        error = rv[0][0]
        error_log = {}
        log_data = cat_file('%s/%s.txt' % (HDFSLOGDIR, uuid))
        if(not log_data):
            return gen_error_response(404, 'No log file for that request')
        error_log['id'] = uuid
        error_log['data'] = log_data
        error_log['type'] = rv[0][1]
        error_log['error'] = rv[0][0]
        return make_response(render_template('error_log.html', error_log=error_log), 200, {'Content-Type': 'text/html'})
