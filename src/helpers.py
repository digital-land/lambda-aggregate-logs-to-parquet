import json

def isJson(myjson):
    """
    This function checks if a string is valid JSON.

    Parameters:
    myjson (str): The string to check.

    Returns:
    bool: True if the string is valid JSON, False otherwise.

    The function tries to parse the string with json.loads. 
    If the parsing succeeds, the function returns True. 
    If a ValueError is raised, the function returns False.
    """
    try:
        json_object = json.loads(myjson)
    except ValueError as e:
        return False
    return True