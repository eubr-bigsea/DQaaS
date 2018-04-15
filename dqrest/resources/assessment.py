import shlex
import uuid

from dqrest.common.authentication import verify_resource, verify_token
from dqrest.common.callback import popen_and_call
from dqrest.common.db import get_db, mark_job_as_done, mark_job_as_error
from dqrest.common.error import gen_error_response
from dqrest.common.hdfs import HDFSCONFIGDIR, HDFSURL
from dqrest.resources.config import make_config_file, verify_config_json
from flask import (Response, json, make_response, render_template, request,
                   url_for)
from flask_restful import Resource


class Assessment(Resource):
    """Resource that provide access to the DQAssessment module

    Available methods: GET/POST
    """

    def get(self, uuid=None):
        if(not uuid):
            return gen_error_response(400, "You should provide a valid uuid")
        cur = get_db().cursor()
        rv = cur.execute(
            'SELECT * FROM requests WHERE uuid = ?', (uuid,)).fetchall()
        if(len(rv) == 0):
            return gen_error_response(404, 'Wrong uuid')
        file_id = cur.execute(
            'SELECT file_id FROM requests_assessment WHERE uuid = ?', (uuid,)).fetchall()[0][0]
        assessment_data = {}
        assessment_data['user_id'] = rv[0][0]
        assessment_data['type'] = rv[0][1]
        assessment_data['uuid'] = rv[0][2]
        assessment_data['done'] = rv[0][3]
        assessment_data['submit_timestamp'] = rv[0][4]
        assessment_data['completed_timestamp'] = rv[0][5]
        assessment_data['error'] = rv[0][6]
        assessment_data['file_id'] = file_id
        headers = {'Content-Type': 'text/html'}
        return make_response(render_template('assessment_detail.html', assessment_data=assessment_data), 200, headers)

    def post(self):
        request_json = request.get_json() or None  # Get json data from the post request
        if request_json:
            # try to read the user security information
            token = request.headers['Authorization'] or None
            if not token:
                return gen_error_response(401, "Authenticate at https://eubrabigsea.dei.uc.pt/engine/api/checkin_data", "DQAssessment")
            username, description = verify_token(token)
            if (username):
                # The user is logged correctly in the system
                isAutorized, description = verify_resource(
                    username, token, "DQAssessment", "DataQuality")  # TODO: cambiare in spark_assessment
                if (isAutorized):
                    # The user is authorized to use the resource
                    isOk, description = verify_config_json(request_json)
                    if(not isOk):
                        return gen_error_response(400, description)
                    file_id, new = make_config_file(request)
                    if(new):
                        cur = get_db().cursor()
                        cur.execute('INSERT INTO assessment_file VALUES(?,?,CURRENT_TIMESTAMP);',
                                    (username, file_id))
                        cur.close()
                    pid = submit_assessment(file_id, username)
                    if(pid):
                        cur = get_db().cursor()
                        cur.execute(
                            'INSERT INTO requests_assessment VALUES(?,?);', (pid, file_id))
                        cur.close()
                        body = {}
                        body["response"] = "Job sucsesfully submitted"
                        body["job_uri"] = url_for('assessment', uuid=pid)
                        body["config_file_uri"] = url_for(
                            'config', file_id=file_id)
                        body = json.dumps(body)
                        return Response(response=body, mimetype="application/json")

                else:
                    # The user is not authorized to use the resource
                    return gen_error_response(403, description, "DQAssessment")
            else:
                # The user is not logged correctly in the system
                return gen_error_response(401, description, "DQAssessment")

        else:
            return gen_error_response(400, "Malformed request")


def on_assessment_done(pid, error):
    """Simple function to provide as callback for the assessment request

    This function, depending on the exit code of the assessment process, calls the proper db function.
    Note: This function has been kept separated from the profiling one for future possible new functions, specific to the assessment.

    Args:
        pid: Unique identifier of the request (uuid4).
        error: Exit code of the request.
    """
    if(not error):
        mark_job_as_done(pid)
    else:
        mark_job_as_error(pid)


def submit_assessment(file_id, username):
    """Submit the assessment on the mesos cluster

    Args:
        file_id: Unique identifier of a configuration file (uuid4), the file must be properly stored on the appropriate HDFSCONFIGDIR location.
        username: The username of the user that is requesting the assessment.

    Returns:
        The function returns the unique identifier of the request (uuid4), None if the assessment has not been properly submitted.
    """
    try:
        profiler = '/spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 /bigSEA/DQAssessment.py '
        config_file = '%s/%s/%s.txt' % (HDFSURL, HDFSCONFIGDIR, file_id)
        raw_command = profiler + " " + config_file + " 1"
        args = shlex.split(raw_command)
        print(args)
        pid = str(uuid.uuid4())
        popen_and_call(on_assessment_done, pid, args)
        try:
            cur = get_db().cursor()
            cur.execute(
                'INSERT INTO requests VALUES (?,"ASSESSMENT",?, 0,CURRENT_TIMESTAMP,? , 0);', (username, str(pid), None))
            cur.close()
        except Exception as e:
            print(e)
        return pid
    except Exception as e:
        print(e)
        return None
