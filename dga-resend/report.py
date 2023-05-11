from database import Database
import json
from datetime import datetime
from task import Task
import os
import requests
from helper import  Helper


class Report(Database):
    def __init__(self):
        super().__init__()
        self.clients = self.get_clients()
        self.helper = Helper()

    def resend_reports(self):
        reports = self.handle_reports()
        for report in reports:
            task = Task(report['type'][0])
            task.create_task_on_queue(self.generate_payload_for_service(report))
        return reports

    def generate_payload_for_service(self, report):
        type = report['type'][0]
        report['callback'] = type['callback']
        payload = {}
        for field in type['required_payload']['payload']:
            if field in report:
                payload[field] = report[field]
        if not 'callback_data' in report:
            payload['callback_data'] = report

        payload['resend'] = True
        self.helper.print(json.dumps(payload, default=str), 2)
        return payload

    def handle_reports(self):
        reports = self.get_reports()
        sensors = self.get_sensors_reported()
        for report in reports:
            self.execute(f"update checkpoint_reports set retries = (retries::int + 1) where id = {report['id']}")
            self.helper.print(f"update checkpoint_reports set retries = (retries::int + 1) where id = {report['id']}", 2)
            report['sensors'] = list(filter(lambda row: row['uuid'] == report['uuid'], sensors))
            rep_date = report['report_date']
            report['report_date'] = rep_date.strftime('%Y-%m-%d')
            report['report_time'] = rep_date.strftime('%H:%M:%S')
            for type_field in report['type_fields'].keys():
                report[type_field] = report['type_fields'][type_field]

            if report['type'][0]['authentication_url'] is not None:
                client = list(filter(lambda row: row['id'] == report['client_id'], self.clients))[0]
                if client['last_reportable_token'] is None or self.calculate_time_of_token(client):
                    report = self.get_token_for_client(report)
                else:
                    report['last_token_used'] = client['last_reportable_token']
                    report['token_expiration'] = client['last_reportable_token_expiration']

            for sensor in report['sensors']:
                if 'fields' in report['type'][0]['sensor_fields']:
                    for field in report['type'][0]['sensor_fields']['fields']:
                        sensor[field] = sensor['sensor_fields']['fields'][field]
                value = sensor['read_value']
                if sensor['turn_to_positive']:
                    value = value * -1
                if sensor['decimals'] == 0:
                    value = int(value)
                else:
                    value = round(value, sensor['decimals'])
                if sensor['link_to'] is not None:
                    report[sensor['link_to']] = value
                sensor['reported_value'] = value
                sensor['report_value'] = value
        return reports

    def get_reports(self):
        sql = f"""
            select 
                c.*, 
                cr.type_id,
                cr.type_fields,
                cr.username,
                cr.password,
                cr.recalculable
            from 
                checkpoint_reports c
                left join checkpoint_reportables cr on c.reportable_id = cr.id
            where 
                c.sync = 0 and 
                cr.type_id = 2 and 
                c.retries <= {int(os.environ.get('MAX_RETRIES', 3))} and
                date(c.report_date) > '2023-04-30'
        """
        types = self.get_reportable_types()
        reports = self.all(sql)

        for report in reports:
            report['type'] = list(filter(lambda row: row['id'] == report['type_id'], types))
        return reports

    def get_sensors_reported(self):
        sql = f"""
            select 
                crs.*,
                rs.link_to,
                rs.turn_to_positive,
                rs.decimals,
                rs.sensor_fields
            from
                checkpoint_reported_sensors crs
                left join reportable_sensors rs on crs.reportable_sensor_id = rs.id
            where 
                uuid in (
                    select 
                        c.uuid
                    from
                        checkpoint_reports c 
                        left join checkpoint_reportables cr on c.reportable_id = cr.id 
                    where 
                        c.sync = 0 and
                        cr.type_id = 2 and 
                        c.retries <= {int(os.environ.get('MAX_RETRIES', 3))} and
                        date(c.report_date) > '2023-04-30'
                )
        """
        return self.all(sql)

    def get_reportable_types(self):
        sql = f"""
            select * from reportable_types
        """
        return self.all(sql)

    def get_token_for_client(self, report):
        data = json.dumps({"usuario": report['username'], "password": report['password']})
        headers = {
            'Content-Type': "application/json"
        }
        response = requests.post(url=report['type'][0]['authentication_url'], data=data, headers=headers)
        content = json.loads(response.content)
        report['last_token_used'] = content['token']
        report['token_expiration'] = content['expiration']

        self.execute(f"""
        update 
            clients 
        set 
            last_reportable_token = '{content['token']}', 
            last_reportable_token_expiration = '{content['expiration']}'
        where id = {report['client_id']}
        """)
        self.clients = self.get_clients()
        return report

    def calculate_time_of_token(self, client):
        date = client['last_reportable_token_expiration']
        return ((date - datetime.now()).total_seconds()) < 120

    def get_clients(self):
        return self.all("select * from clients")