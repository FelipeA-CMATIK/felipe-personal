from task import Task
import json
from datetime import datetime
from helper import Helper


class Setup:
    def __init__(self):
        super().__init__()
        self.helper = Helper()

    def setup_report(self, request):
        type = request['type'][0]
        recalculated = False
        recalculated_value = 0

        for sensor in request['sensors']:
            if 'fields' in type['sensor_fields']:
                for field in type['sensor_fields']['fields']:
                    sensor[field] = sensor['sensor_fields']['fields'][field]
            value = sensor['report_value']
            if sensor['report_value'] is None:
                value = sensor['last_report_value']
            else:
                if sensor['turn_to_positive']:
                    value = value * -1
            if value < 0:
                sensor['report_value'] = 0
            else:
                if sensor['decimals'] == 0:
                    sensor['report_value'] = int(value)
                else:
                    sensor['report_value'] = '{:.{decimals}f}'.format(value, decimals=sensor['decimals'])
            if sensor['link_to'] is not None:
                if type['name'] == 'DGA' and sensor['link_to'] == 'tote' and request['recalculable']:
                    recalculated = True
                    recalculated_value = self.dga_recalculated_value(request, sensor)

                if sensor['link_to'] == 'flow' and float(sensor['report_value']) < 0:
                    request[sensor['link_to']] = 0
                else:
                    request[sensor['link_to']] = sensor['report_value']

            if recalculated:
                if type['name'] == 'DGA':
                    request['flow'] = recalculated_value

        payload = self.generate_payload_for_service(request)
        task = Task(type)
        task.create_task_on_queue(payload)
        return payload

    def dga_recalculated_value(self, request, sensor):
        value = (((sensor['report_value'] - sensor['last_report_value'])*1000) / request['frequency_in_seconds'])
        if float(value) < 0:
            value = 0.00
        return '{:.{decimals}f}'.format(value, decimals=2)

    def generate_payload_for_service(self, request):
        type = request['type'][0]
        request['callback'] = type['callback']
        payload = {}
        for field in type['required_payload']['payload']:
            if field in request:
                payload[field] = request[field]
            elif field in request['type_fields']:
                payload[field] = request['type_fields'][field]
        payload['callback_data'] = request
        return payload
