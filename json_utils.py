## json_utils.py
import json
import re

def extract_json_data(data):
    """
    Extract JSON data from a string.

    Args:
        data (str): The input string to extract JSON data from.

    Returns:
        dict: The extracted JSON data as a Python dictionary, or an empty dictionary if no JSON data is found.
    """
    json_data = {}

    # Try to parse the entire string as JSON
    try:
        json_data = json.loads(data)
    except json.JSONDecodeError:
        pass

    # Extract JSON code blocks from Markdown syntax
    match = re.search(r'```json([\s\S]*?)```', data)
    if match:
        json_string = match.group(1).strip()
        try:
            json_data = json.loads(json_string)
        except json.JSONDecodeError:
            pass

    # Extract JSON objects from the string
    if not json_data:
        first_open = -1
        while True:
            first_open = data.find('{', first_open + 1)
            if first_open == -1:
                break
            first_close = data.rfind('}')
            while first_close <= first_open:
                first_close = data.rfind('}', 0, first_close)
            candidate = data[first_open:first_close + 1]
            try:
                json_data = json.loads(candidate)
                break
            except json.JSONDecodeError:
                first_close = data.rfind('}', 0, first_close)

    # Extract JSON arrays from the string
    if not json_data:
        first_open = -1
        while True:
            first_open = data.find('[', first_open + 1)
            if first_open == -1:
                break
            first_close = data.rfind(']')
            while first_close <= first_open:
                first_close = data.rfind(']', 0, first_close)
            candidate = data[first_open:first_close + 1]
            try:
                json_data = json.loads(candidate)
                break
            except json.JSONDecodeError:
                first_close = data.rfind(']', 0, first_close)

    return json_data
