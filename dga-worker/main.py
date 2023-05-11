import json
from setup import Setup


def setup_reports(request):
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': "*",
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)



    headers = {
        'Access-Control-Allow-Origin': "*"
    }

    try:
        request = request.get_json()
        setup = Setup()
        payload = setup.setup_report(request)
        return json.dumps(payload), 200, headers

    except Exception as error:
        return json.dumps(error), 422, headers
        raise
