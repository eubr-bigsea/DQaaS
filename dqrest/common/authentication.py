import requests


def verify_token(token):
    """Check if the token is valid.

    Check if the token corresponds to an authenticated user.
    Note that this function does not check whether the user is authorized
    to execute a task.

    Args:
        token: the string provided by the user in the Authentication header,
            (it may start with 'Bearer').
    Returns:
        In case of a positive authentication it returns the response
        provided by the authentication service and "Ok", None and the error
        description otherwise.
    """
    if(token.startswith('Bearer')):
        token = token.split(' ')[1]
    check_token_url = 'https://eubrabigsea.dei.uc.pt/engine/api/verify_token'
    data = "token=" + token
    r = requests.post(check_token_url, data)
    response = r.json()
    response = response["response"].encode("utf-8")
    if(response != "invalid token"):
        return response, "Ok"
    else:
        return None, "It seems that your token is not valid, try to login in the system"


def verify_resource(username, token, resource_name, resource_category):
    """Check if the user is authorized to use a resource.

    Verifies by accessing the security module if the user is authorized to
    execute the resource identified by 'resource_name'.

    Args:
        username: A string with the username, it can be retrieved from the verify_token response.
        token: The token provided by the user.
        resource_name: The name of the resource that the user want to access to.
        resource_category: The category that groups the resource.

    Returns:
        If the user is authorized it returns True and a dict with the response parameters got from the security service,
        otherwise False and a string with the error message.
    """

    if(token.startswith('Bearer')):
        token = token.split(' ')[1]
    use_resource_url = 'https://eubrabigsea.dei.uc.pt/engine/api/use_resource'
    data = "username=" + username + "&resource_name=" + resource_name + \
        "&resource_category=" + resource_category + "&token=" + token
    r = requests.post(use_resource_url, data)
    response = r.json()
    if("success" in response):
        response = response["success"].encode("utf-8")
        return True, response
    else:
        return False, "You are not authorized"
