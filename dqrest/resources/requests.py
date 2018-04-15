from flask import make_response, render_template
from flask_restful import Resource
from dqrest.common.db import get_db

class Requests(Resource):

    def get(self):
        cur = get_db().cursor()
        rv = cur.execute('SELECT * FROM requests').fetchall()
        request_list = []
        for element in rv:
            if element[1] == 'ASSESSMENT':
                file_id = cur.execute(
                    'SELECT file_id FROM requests_assessment WHERE uuid = ?', (element[2],)).fetchall()[0][0]
                element = element + (file_id,)
            request_list.append(element)
        headers = {'Content-Type': 'text/html'}
        return make_response(render_template('requests.html', request_list=request_list), 200, headers)