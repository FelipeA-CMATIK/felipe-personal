import json
from report import Report
from helper import Helper


def finish_reports(request):
    helper = Helper()
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
        helper.print(request, 1)
        report = Report()
        payload = report.update_report(request)
        return json.dumps(payload), 200, headers

    except Exception as error:
        return json.dumps(error), 422, headers
        raise