from datetime import datetime
import json
import requests
import xmltodict
import xml.etree.ElementTree as elementTree
from task import Task
from zoneinfo import ZoneInfo
import os
import time
from helper import Helper


class Soap:
    def __init__(self):
        self.helper = Helper()
        self.wsdl = "https://snia.mop.gob.cl/controlextraccion/datosExtraccion/SendDataExtraccionService?wsdl.SendDataExtraccionPort"

    def is_xml(self, value):
        try:
            elementTree.fromstring(value)
        except elementTree.ParseError:
            return False
        return True

    def send_request(self, request):
        time.sleep(15)
        time_start = datetime.now()
        if 'level' in request and float(request['level']) < 0:
            request['level'] = request['level'] * -1
        body = self.get_body(request)
        self.helper.print(body, 1)
        headers = {
            "content-type": 'application/soap+xml',
        }

        rsp = requests.post(self.wsdl, data=body, headers=headers)
        time_end = datetime.now()

        if self.is_xml(rsp.content):
            content = self.parse_response(rsp.content)
        else:
            self.helper.print({
                'error': "la respuesta del servicio no está en formato XML"
            }, 1)
            self.helper.print(rsp.content, 1)
            return {
                'error': "la respuesta del servicio no está en formato XML"
            }

        response = dict()
        response['uuid'] = request['uuid']
        response['response_code'] = content['status']['Code']
        response['response_message'] = content['status']['Description']
        response['response_text'] = ''

        self.helper.print(rsp.content, 1)
        if content['status']['Code'] == '0':
            if 'resend' in request:
                response['sync'] = 2
            elif 'remake' in request:
                response['sync'] = 3
            else:
                response['sync'] = 1
            response['verification_code'] = content['numeroComprobante']
        else:
            response['verification_code'] = ''
            response['sync'] = 0
        response['start'] = time_start.strftime('%Y-%m-%d %H:%M:%S')
        response['end'] = time_end.strftime('%Y-%m-%d %H:%M:%S')
        response['reported_at'] = self.fix_timezone(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
        response['reported_body'] = ''
        response['reported_body_json'] = request
        response['uri_used'] = self.wsdl

        task = Task(response['reported_body_json']['callback'])
        task.create_task_on_queue(response)
        self.helper.print(json.dumps(response, default=str), 1)
        return response

    def fix_timezone(self, date):
        return date.astimezone(ZoneInfo(os.environ.get('TIMEZONE')))

    def parse_response(self, content):
        response = dict(xmltodict.parse(content))
        self.helper.print(response, 1)
        return response['soapenv:Envelope']['soapenv:Body']['authSendDataExtraccionResponse']

    def get_body(self, request):
        body = f"""
            <x:Envelope
                xmlns:x="http://schemas.xmlsoap.org/soap/envelope/"
                xmlns:aut1="http://www.mop.cl/controlextraccion/xsd/datosExtraccion/AuthSendDataExtraccionRequest">
                <x:Header>
                   <aut1:authSendDataExtraccionTraza>
                       <aut1:codigoDeLaObra>{request['work_code']}</aut1:codigoDeLaObra>
                       <aut1:timeStampOrigen>{datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")}</aut1:timeStampOrigen>
                   </aut1:authSendDataExtraccionTraza>
                </x:Header>
                <x:Body>
                   <aut1:authSendDataExtraccionRequest>
                       <aut1:authDataUsuario>
                           <aut1:idUsuario>
                               <aut1:rut>{request['username']}</aut1:rut>
                           </aut1:idUsuario>
                           <aut1:password>{request['password']}</aut1:password>
                       </aut1:authDataUsuario>
                       <aut1:authDataExtraccionSubterranea>
                           <aut1:fechaMedicion>{datetime.strptime(request['report_date'], "%Y-%m-%d").strftime("%d-%m-%Y")}</aut1:fechaMedicion>
                           <aut1:horaMedicion>{request['report_time']}</aut1:horaMedicion>
                           <aut1:totalizador>{request['tote']}</aut1:totalizador>
                           <aut1:caudal>{request['flow']}</aut1:caudal>
                           <aut1:nivelFreaticoDelPozo>{request['level']}</aut1:nivelFreaticoDelPozo>
                       </aut1:authDataExtraccionSubterranea>
                   </aut1:authSendDataExtraccionRequest>
                </x:Body>
            </x:Envelope>
        """.strip().lstrip().rstrip().replace('\n', '').encode('utf-8')
        self.helper.print(body, 1)
        return body

    def extract_date(self, date):
        return datetime.strptime(date, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y")

    def extract_hour(self, date):
        return datetime.strptime(date, "%Y-%m-%d %H:%M:%S").strftime("%H:%M:%S")
