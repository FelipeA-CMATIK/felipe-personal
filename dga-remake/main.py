import json
from report import Report


def remake_reports(request):
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
        report = Report()
        payload = report.remake_reports(request)

        return payload, 200, headers
    except Exception as error:
        return json.dumps(error), 401, headers
       