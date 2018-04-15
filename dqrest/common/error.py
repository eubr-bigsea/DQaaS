from flask import Response, json


def gen_error_response(code, description, realm=None):
    """Generate a proper response for HTTP status code 400, 401, 403, 404

    Args:
        code: HTTP status code.
        description: Message to describe the error, provided to the user as 'error_description' body field.
        realm: Realm for erorr 401 or 403, default None.

    Returns:
        A flask Response class.
    """
    if realm:
        bearer = 'Bearer realm="' + realm + '"'
        header = {}
        header["WWW-Authenticate"] = bearer
    msg = ""
    if(code == 400):
        msg = "invalid_request"
    elif(code == 401):
        msg = "invalid_token"
    elif(code == 403):
        msg = "insufficient_scope"
    elif(code == 404):
        msg = "not_found"
    body = {}
    body["error"] = msg
    body["error_descriprion"] = description
    body = json.dumps(body)
    if realm:
        return Response(response=body, status=code, headers=header, mimetype="application/json")
    else:
        return Response(response=body, status=code, mimetype="application/json")
