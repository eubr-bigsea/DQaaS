import shlex
import uuid

from dqrest.common.authentication import verify_resource, verify_token
from dqrest.common.callback import popen_and_call
from dqrest.common.db import get_db, mark_job_as_done, mark_job_as_error
from dqrest.common.error import gen_error_response
from flask import (Response, json, make_response, render_template, request,
                   url_for)
from flask_restful import Resource


class Profiling(Resource):
    """Resource that provide access to the DQProfiling module

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
        profiling_details = cur.execute(
            'SELECT * FROM requests_profiling WHERE uuid = ?', (uuid,)).fetchall()
        if(len(profiling_details) == 0):
            return gen_error_response(404, 'Wrong uuid')
        args_string = profiling_details[0][1]
        profiling_data = {}
        profiling_data['user_id'] = rv[0][0]
        profiling_data['type'] = rv[0][1]
        profiling_data['uuid'] = rv[0][2]
        profiling_data['done'] = rv[0][3]
        profiling_data['submit_timestamp'] = rv[0][4]
        profiling_data['completed_timestamp'] = rv[0][5]
        profiling_data['args_dict'] = json.loads(args_string)
        profiling_data['error'] = rv[0][6]
        headers = {'Content-Type': 'text/html'}
        return make_response(render_template('profiling_detail.html', profiling_data=profiling_data), 200, headers)

    def post(self):
        request_json = request.get_json()  # Get json data from the post request
        if request_json:
            # try to read the user security information
            token = request.headers['Authorization'] or None
            if not token:
                return gen_error_response(401, "Authenticate at https://eubrabigsea.dei.uc.pt/engine/api/checkin_data", "DQProfiling")
            username, description = verify_token(token)
            if (username):
                # The user is logged correctly in the system
                isAutorized, description = verify_resource(
                    username, token, "DQProfiling", "DataQuality")
                if (isAutorized):
                    # The user is authorized to use the resource
                    pid = submit_profiling(request, username)
                    cur = get_db().cursor()
                    cur.execute(
                        'INSERT INTO requests_profiling VALUES(?,?);', (pid, json.dumps(request_json)))
                    cur.close()
                    if(pid):
                        body = {}
                        body["response"] = "Job sucsesfully submitted"
                        body["job_uri"] = url_for('profiling', uuid=pid)
                        body = json.dumps(body)
                        return Response(response=body, mimetype="application/json")
                else:
                    # The user is not authorized to use the resource
                    return gen_error_response(403, description, "DQProfiling")
            else:
                # The user is not logged correctly in the system
                return gen_error_response(401, description, "DQProfiling")

        else:
            return gen_error_response(400, "Malformed request")


def on_profiling_done(pid, error):
    """Simple function to provide as callback for the profiling request

    This function, depending on the exit code of the profiling process, calls the proper db function.
    Note: This function has been kept separated from the assessment one
    for future possible new functions, specific to the assessment.

    Args:
        pid: Unique identifier of the request (uuid4).
        error: Exit code of the request.
    """
    if(not error):
        mark_job_as_done(pid)
    else:
        mark_job_as_error(pid)


def submit_profiling(request, username):
    """Submit the profiling on the mesos cluster

    Args:
        request: The flask request that generated the profiling request.
            It must contain all the fields required to submit a profiling.
        username: The username of the user that is requesting the assessment.

    Returns:
        The function returns the unique identifier of the request (uuid4),
        None if the assessment has not been properly submitted.
    """
    try:
        profiler = '/spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 /bigSEA/DQProfiling.py '
        json = request.get_json()
        intervalString = ""

        if(isSetPattern(json)):
            input_file = "--pattern=" + json["pattern"]
            fromFile = json["from"]
            toFile = json["to"]
            interval = json["interval"]
            intervalString = fromFile + ";" + toFile + ";" + interval

        elif("input" in json):
            input_file = json["input"]

        output_file = json["output"]
        timeliness = json["timeliness"]
        timeformat = json["time_format"]
        timeformat = '"' + timeformat + '"'
        consistency_rules = json["consistency_rules"]
        consistency_rules = '"' + consistency_rules + '"'
        raw_command = profiler + " " + input_file + " " + output_file + " " + \
            timeliness + " " + timeformat + " " + consistency_rules + " " + intervalString
        args = shlex.split(raw_command)
        print(args)
        pid = str(uuid.uuid4())
        popen_and_call(on_profiling_done, pid, args)
        try:
            cur = get_db().cursor()
            cur.execute(
                'INSERT INTO requests VALUES (?,"PROFILING",?, 0,CURRENT_TIMESTAMP, ?, 0);', (username, str(pid), None))
            cur.close()
        except Exception as e:
            print(e)
        return pid
    except Exception as e:
        print(e)
        return None


def isSetPattern(json):
    """Helper function to check if the request is for an interval of days or for a single day.

    Args:
        json: json file from the body of the request.
    
    Returns:
        True if the request is for an interval, False if it is for a single day.
    """
    if ("pattern" not in json):
        return False
    if ("from" not in json):
        return False
    if ("to" not in json):
        return False
    if ("interval" not in json):
        return False
    return True
