from datetime import datetime
import json
import requests
import xmltodict
import xml.etree.ElementTree as elementTree
from task import Task
from zoneinfo import ZoneInfo
import os
import re


class Soap:
    def __init__(self):
        self.wsdl = "https://snia.mop.gob.cl/controlextraccion/datosExtraccion/SendDataExtraccionService?wsdl.SendDataExtraccionPort"

    def is_xml(self, value):
        try:
            elementTree.fromstring(value)
        except elementTree.ParseError:
            return False
        return True

    def verify_report(self, request):
        body = self.get_body(request)
        print(body)
        headers = {
            "content-type": 'text/xml',
            "SOAPAction": 'urn:getDataExtraccionOp'
        }

        rsp = requests.post(self.wsdl, data=body, headers=headers)

        if self.is_xml(rsp.content):
            content = self.parse_response(rsp.content)
            print(content)
            response = dict()
            response['uuid'] = request['uuid']
            if 'medicion' in content and content['medicion'] is not None:

                response['reported_at'] = content['medicion']['fechaIngreso']
                response['report_date'] = content['medicion']['fechaMedicion']
                response['flow'] = content['medicion']['caudal']
                response['tote'] = content['medicion']['totalizador']
                response['level'] = content['medicion']['nivelFreaticoDelPozo']

            response['verification_code'] = content['status']['Code']
            response['verification_message'] = content['status']['Description']
            print(json.dumps(response, default=str))
            print('callback', request['callback'])
            task = Task(request['callback'])
            task.create_task_on_queue(response)

            return response
        else:
            return {
                'error': "la respuesta del servicio no está en formato XML"
            }

    def fix_timezone(self, date):
        return date.astimezone(ZoneInfo(os.environ.get('TIMEZONE')))

    def parse_response(self, content):
        response = dict(xmltodict.parse(content))
        print('response parsed', response)
        return response['soapenv:Envelope']['soapenv:Body']['getDataExtraccionResponse']

    def get_body(self, request):
        return f"""
            <soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' xmlns:get='http://www.mop.cl/controlextraccion/xsd/datosExtraccion/GetDataExtraccionRequest'>
                <soapenv:Header/>
                <soapenv:Body>
                    <get:getDataExtraccionRequest>
                        <get:codigoDeLaObra>{request['work_code']}</get:codigoDeLaObra>
                        <get:numeroComprobante>{request['verification_code']}</get:numeroComprobante>
                    </get:getDataExtraccionRequest>
                </soapenv:Body>
            </soapenv:Envelope>
        """.strip().lstrip().rstrip().replace('\n', '').encode('utf-8')


