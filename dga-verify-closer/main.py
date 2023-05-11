import json
from report import Report


def finish_verify(request):
    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
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

        report.close_verification(request)
        return json.dumps({"message": "Reporte Verificado"}), 200, headers

    except Exception as error:
        return json.dumps(error), 422, headers
        raise