import json
from soap import Soap
from helper import Helper


def send_reports(request):
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
        soap = Soap()
        helper = Helper()
        helper.print(request, 1)
        soap.send_request(request)

        return json.dumps({"message": "Enviado a DGA."}), 200, headers
    except Exception as error:
        return json.dumps(error), 401, headers
       