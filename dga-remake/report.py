from database import Database
from helper import Helper
from bigquery import BigQuery
from datetime import datetime, timedelta, date as d
import uuid
import json
import requests
from task import Task


class Report(Database):
    def __init__(self):
        super().__init__()
        self.helper = Helper()
        self.bq = BigQuery()
        self.reportables = []
        self.sensors = []
        self.request = []
        self.clients = self.get_clients()
        self.columns = [
            'uuid',
            'reportable_sensor_id',
            'reported_value',
            'read_value',
            'read_date'
        ]
        self.frequency = None
        self.table = "checkpoint_reported_sensors"

    def remake_reports(self, request):
        self.request = request
        if 'fix' in self.request:
            date_new = self.helper.fix_timezone(datetime.now())
            new_date = datetime.strftime(date_new, "%Y-%m-%d")
            self.request = {
                'date': new_date,
                'type': "DGA",
                'send': True
            }
        if 'send' in self.request:
            if 'extended' in self.request:
                self.send_to_reportable(self.get_extended_data_to_report())
            else:
                self.send_to_reportable(self.get_data_to_report(self.request))
            return json.dumps({
                'message': "Reportes enviados"
            })
        else:
            if 'extended' in self.request:
                return self.print_as_json(self.get_extended_data_to_report())
            else:
                return self.print_as_json(self.get_data_to_report(self.request))

    def get_extended_data_to_report(self):
        reportables = self.get_extended_reportables_with_sensors_and_reports()
        date = self.calculate_extended_date()
        filtered_reportables = list(
            filter(lambda row: 'missing_reports' in row and len(row['missing_reports']) > 0, reportables))

        sensors = self.get_sensors_from_filtered_reportables(filtered_reportables)

        if len(sensors) > 0:
            data_from_bq = self.bq.get_rows(self.helper.pluck(sensors, 'sensor_id'), date['start_date'],
                                            date['end_date'])
            rows = [dict(row) for row in data_from_bq]

            for reportable in filtered_reportables:
                to_report_data = []
                for missing_report_date in reportable['missing_reports']:
                    rep_data = {}
                    non_fixed_date = datetime.strptime(missing_report_date, '%Y-%m-%d %H:%M')
                    date = datetime.strptime(missing_report_date, '%Y-%m-%d %H:%M') - timedelta(
                        hours=(self.frequency['in_seconds']) / 3600) + timedelta(minutes=15)

                    rep_data['date'] = non_fixed_date
                    rep_data['sensors'] = []

                    for sensor in reportable['sensors']:
                        ss = {
                            'turn_to_positive': sensor['turn_to_positive'],
                            'sensor_id': sensor['sensor_id'],
                            'id': sensor['id'],
                            'decimals': sensor['decimals'],
                            'link_to': sensor['link_to']
                        }
                        report_data = list(filter(
                            lambda row: (
                                    row['sensor_id'] == ss['sensor_id'] and
                                    row['date'].strftime("%Y-%m-%d %H:%M") <= missing_report_date
                            )
                            , rows)
                        )
                        if len(report_data) > 0:
                            value = report_data[-1]['result']
                            if ss['turn_to_positive']:
                                value = value * -1

                            if ss['decimals'] == 0:
                                value = int(value)
                            else:
                                value = '{:.{decimals}f}'.format(value, decimals=ss['decimals'])

                            hour = int(date.strftime("%H"))
                            if hour == 0:
                                if ss['link_to'] == 'flow':
                                    ss['last_report_value'] = 0
                                else:
                                    ss['last_report_value'] = value
                                ss['last_report_date'] = report_data[0]['date']
                            else:
                                new_hour = '{:02}'.format(hour)
                                fmt = f"%Y-%m-%d {new_hour}:00"
                                fixed_fmt = f"%Y-%m-%d {new_hour}:15"
                                last_report_date = date.strftime(fmt)
                                fixed_last_report_date = date.strftime(fixed_fmt)
                                ss['last_report_date'] = last_report_date
                                last_report = list(filter(
                                    lambda row: (
                                            row['sensor_id'] == ss['sensor_id'] and
                                            row['date'].strftime("%Y-%m-%d %H:%M") <= fixed_last_report_date
                                    )
                                    , rows)
                                )
                                if len(last_report) > 0:
                                    last_value = last_report[0]['result']
                                else:
                                    last_value = value
                                if ss['turn_to_positive']:
                                    last_value = last_value * -1

                                if ss['decimals'] == 0:
                                    last_value = int(last_value)
                                else:

                                    if last_value == '':
                                        last_value = 0

                                    if sensor['decimals'] == '':
                                        sensor['decimals'] = 0

                                    last_value = '{:.{decimals}f}'.format(float(last_value),
                                                                          decimals=int(sensor['decimals']))
                                ss['last_report_value'] = last_value

                            ss['report_value'] = value
                            ss['report_date'] = report_data[-1]['date']
                            print("ss", ss)
                            rep_data['sensors'].append(ss)
                    to_report_data.append(rep_data)

                reportable['to_report_data'] = to_report_data
            return filtered_reportables
        else:
            return []

    def get_extended_reportables_with_sensors_and_reports(self):
        reportables = self.get_extended_reportables()
        self.request['reportables'] = self.helper.pluck(reportables, 'id')
        reports = self.get_reports_sent(self.request)

        sensors = self.get_sensors()

        frequencies = self.get_frequencies()
        for reportable in reportables:
            reportable['sensors'] = list(filter(lambda row: row['reportable_id'] == reportable['id'], sensors))
            reportable['reports_sent'] = list(filter(lambda row: row['reportable_id'] == reportable['id'], reports))
            frequency = list(filter(lambda row: row['id'] == reportable['frequency_id'], frequencies))[0]
            reportable['frequency_in_seconds'] = frequency['in_seconds']
            reportable['must_report_count'] = 1
            reportable['sent_reports_count'] = len(reportable['reports_sent'])
            if reportable['sent_reports_count'] != 1:
                reportable['missing_reports'] = [f"{self.request['date']} 12:00"]
        return reportables

    def send_to_reportable(self, reportables):
        types = self.get_types()
        payload_count = 0
        total_reps = 0
        data = []
        for reportable in reportables:
            reportable['reports_sent'] = ''
            type = list(filter(lambda row: row['id'] == reportable['type_id'], types))[0]
            if type['authentication_url'] is not None:
                client = list(filter(lambda row: row['id'] == reportable['client_id'], self.clients))[0]
                if client['last_reportable_token'] is None or self.calculate_time_of_token(client):
                    reportable = self.get_token_for_client(reportable)
                else:
                    reportable['last_token_used'] = client['last_reportable_token']
                    reportable['token_expiration'] = client['last_reportable_token_expiration']

            for type_field in reportable['type_fields'].keys():
                reportable[type_field] = reportable['type_fields'][type_field]

            if type['id'] == 1:
                reportable['type'] = [type]
            else:
                reportable['type'] = type

            recalculated = False
            recalculated_value = 0
            total_reps += len(reportable['to_report_data'])
            for report_data in reportable['to_report_data']:
                reportable['uuid'] = uuid.uuid4()
                sql = f"""
                               INSERT INTO checkpoint_reports (
                                   reportable_id,
                                   uuid,
                                   report_date,
                                   sync,
                                   created_at      
                               ) VALUES (
                                   {reportable['id']},
                                   '{reportable['uuid']}',
                                   '{report_data['date']}',
                                   0,
                                   '{self.helper.fix_timezone(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')}'
                               )
                       """
                # print(sql)
                self.execute(sql)
                # print("report data sensors:", report_data['sensors'])
                for sensor in report_data['sensors']:
                    if 'fields' in type['sensor_fields']:
                        for field in type['sensor_fields']['fields']:
                            print("field", field)
                            print(sensor)
                            sensor[field] = sensor['sensor_fields']['fields'][field]
                    value = sensor['report_value']

                    if sensor['report_value'] is None:
                        value = sensor['last_report_value']
                        if value is None:
                            value = 0
                    if sensor['link_to'] is not None:
                        if type['name'] == 'DGA' and sensor['link_to'] == 'tote' and reportable['recalculable']:
                            recalculated = True
                            recalculated_value = self.dga_recalculated_value(reportable, sensor)
                        reportable[sensor['link_to']] = sensor['report_value']

                        if type['name'] == 'DGA' and sensor['link_to'] == 'flow':
                            if float(value) < 0:
                                value = 0

                        if type['name'] == 'DGA' and sensor['link_to'] == 'tote':
                            if float(value) < 0:
                                value = 0

                    if recalculated:
                        if type['name'] == 'DGA':
                            reportable['flow'] = recalculated_value
                    reportable['report_date'] = sensor['report_date'].strftime("%Y-%m-%d")
                    reportable['report_time'] = sensor['report_date'].strftime("%H:%M:%S")

                    data.append({
                        'uuid': reportable['uuid'],
                        'reportable_sensor_id': sensor['id'],
                        'reported_value': None,
                        'read_value': float(value),
                        'read_date': sensor['report_date'],
                    })
                reportable['sensors'] = report_data['sensors']
                payload = self.generate_payload_for_service(reportable, type)
                task = Task(type)

                task.create_task_on_queue(payload)
                print("payload", json.dumps(payload, default=str))
                print("task sent")
            if data:
                self.insert_sensors(data)
                print("sensor data inserted")
            print(json.dumps(data, default=str))
            # redeploy

    def insert_sensors(self, sensors_data):
        print("sensors data", sensors_data)
        self.execute_with_payload(
            self.massive_insert_statement(len(sensors_data)),
            self.flatten(sensors_data)
        )
        print("sensores insertados")

    def dga_recalculated_value(self, request, sensor):
        value = (((sensor['report_value'] - sensor['last_report_value']) * 1000) / request['frequency_in_seconds'])
        return '{:.{decimals}f}'.format(value, decimals=2)

    def generate_payload_for_service(self, request, type):
        request['callback'] = type['callback']
        payload = {}
        for field in type['required_payload']['payload']:
            if field in request:
                payload[field] = request[field]
            elif field in request['type_fields']:
                payload[field] = request['type_fields'][field]
        payload['callback_data'] = request
        payload['remake'] = True
        return payload

    def get_types(self):
        return self.all("select * from reportable_types")

    def calculate_time_of_token(self, client):
        date = client['last_reportable_token_expiration']
        return ((date - datetime.now()).total_seconds()) < 120

    def print_as_json(self, reportables):
        json_data = []
        types = self.get_types()
        for reportable in reportables:
            type = list(filter(lambda row: row['id'] == reportable['type_id'], types))[0]
            if type['name'] == 'DGA':
                for data in reportable['to_report_data']:

                    ss = {
                        'work_code': reportable['type_fields']['work_code'],
                        'date': data['date'].strftime('%Y-%m-%d'),
                        'time': data['date'].strftime('%H:%M:%S')
                    }
                    recalculated = False
                    for sensor in data['sensors']:
                        ss[sensor['link_to']] = sensor['report_value']
                        if sensor['link_to'] is not None:
                            if type['name'] == 'DGA' and sensor['link_to'] == 'tote' and reportable['recalculable']:
                                recalculated = True
                                recalculated_value = self.dga_recalculated_value(reportable, sensor)

                    if recalculated:
                        if type['name'] == 'DGA':
                            ss['flow'] = recalculated_value
                    json_data.append(ss)
            else:
                for data in reportable['to_report_data']:
                    ss = {
                        'process_id': reportable['type_fields']['procesoId'],
                        'ufid': reportable['type_fields']['ufid'],
                        'dispositivoId': reportable['type_fields']['dispositivoId'],
                        'date': data['date'].strftime('%Y-%m-%d'),
                        'time': data['date'].strftime('%H:%M:%S'),
                        'sensors': data['sensors']
                    }

                    json_data.append(ss)
        return json.dumps(json_data, default=str)

    def get_token_for_client(self, reportable):
        data = json.dumps({"usuario": reportable['username'], "password": reportable['password']})
        headers = {
            'Content-Type': "application/json"
        }
        response = requests.post(url=reportable['type'][0]['authentication_url'], data=data, headers=headers)
        content = json.loads(response.content)
        reportable['last_token_used'] = content['token']
        reportable['token_expiration'] = content['expiration']

        self.execute(f"""
        update 
            clients 
        set 
            last_reportable_token = '{content['token']}', 
            last_reportable_token_expiration = '{content['expiration']}'
        where id = {reportable['client_id']}
        """)
        self.clients = self.get_clients()
        return reportable

    def get_data_to_report(self, request):
        reportables = self.get_reportables_with_sensors_and_reports(request)

        filtered_reportables = list(filter(lambda row: len(row['missing_reports']) > 0, reportables))

        sensors = self.get_sensors_from_filtered_reportables(filtered_reportables)

        if len(sensors) > 0:
            data_from_bq = self.bq.get_rows(self.helper.pluck(sensors, 'sensor_id'), request['date'], request['date'])

            rows = [dict(row) for row in data_from_bq]
            for reportable in filtered_reportables:
                to_report_data = []
                for missing_report_date in reportable['missing_reports']:
                    rep_data = {}
                    date = datetime.strptime(missing_report_date, '%Y-%m-%d %H:%M')

                    rep_data['date'] = date
                    rep_data['sensors'] = []

                    for sensor in reportable['sensors']:

                        ss = {
                            'turn_to_positive': sensor['turn_to_positive'],
                            'sensor_id': sensor['sensor_id'],
                            'id': sensor['id'],
                            'decimals': sensor['decimals'],
                            'link_to': sensor['link_to'],
                            'sensor_fields': sensor['sensor_fields']
                        }

                        report_data = list(filter(
                            lambda row: (
                                    row['sensor_id'] == ss['sensor_id'] and
                                    row['date'].strftime("%Y-%m-%d %H:00") <= missing_report_date
                            )
                            , rows)
                        )
                        if len(report_data) > 0:
                            value = report_data[-1]['result']
                            if ss['turn_to_positive']:
                                value = value * -1

                            if ss['decimals'] == 0:
                                value = int(value)
                            else:
                                value = '{:.{decimals}f}'.format(value, decimals=ss['decimals'])

                            hour = int(date.strftime("%H"))
                            if hour == 0:
                                if ss['link_to'] == 'flow':
                                    ss['last_report_value'] = 0
                                else:
                                    ss['last_report_value'] = value
                                ss['last_report_date'] = report_data[-1]['date']
                            else:
                                new_hour = '{:02}'.format(hour - 1)
                                fmt = f"%Y-%m-%d {new_hour}:00"
                                last_report_date = date.strftime(fmt)
                                ss['last_report_date'] = last_report_date
                                last_report = list(filter(
                                    lambda row: (
                                            row['sensor_id'] == ss['sensor_id'] and
                                            row['date'].strftime("%Y-%m-%d %H:00") == last_report_date
                                    )
                                    , rows)
                                )

                                if len(last_report) > 0:
                                    last_value = last_report[-1]['result']
                                else:
                                    last_value = value
                                if ss['turn_to_positive']:
                                    last_value = last_value * -1

                                if ss['decimals'] == 0:
                                    last_value = int(last_value)
                                else:

                                    if last_value == '':
                                        last_value = 0

                                    if sensor['decimals'] == '':
                                        sensor['decimals'] = 0

                                    last_value = '{:.{decimals}f}'.format(float(last_value),
                                                                          decimals=int(sensor['decimals']))
                                ss['last_report_value'] = last_value

                            if ss['link_to'] != 'level':
                                if float(value) < 0:
                                    value = None

                            ss['report_value'] = value
                            ss['report_date'] = report_data[-1]['date']
                            rep_data['sensors'].append(ss)

                    to_report_data.append(rep_data)

                reportable['to_report_data'] = to_report_data
            return filtered_reportables
        else:
            return []

    @staticmethod
    def get_sensors_from_filtered_reportables(reportables):
        sensors = []
        for reportable in reportables:
            for sensor in reportable['sensors']:
                sensors.append(sensor)
        return sensors

    def calculate_missing_reports(self, reportable, request):
        if reportable['must_report_count'] == 24:
            return self.calculate_missing_reports_hourly(reportable, request)
        return self.calculate_missing_reports_daily(reportable, request)

    @staticmethod
    def calculate_missing_reports_daily(reportable, request):
        if reportable['sent_reports_count'] < 1:
            return [request['date'] + " 12:00"]
        return []

    @staticmethod
    def calculate_missing_reports_hourly(reportable, request):
        reports = []
        for i in range(24):
            hour = '{:02}'.format(i)
            report = list(filter(
                lambda row:
                datetime.strftime(row['report_date'], '%H:00') ==
                hour + ':00',
                reportable['reports_sent']
            ))
            if len(report) < 1:
                reports.append(request['date'] + ' ' + hour + ":00")
        if len(reports) > 0:
            return [reports[0]]
        return []

    def get_reportables(self):
        if 'reportables' in self.request:
            sql = f"""
                       select checkpoint_reportables.*,
                       caf.authorized_flow as authorized_flow  from 
                       checkpoint_reportables 
                        left join (select distinct(checkpoint_id), authorized_flow from checkpoint_authorized_flows) as caf on caf.checkpoint_id = checkpoint_reportables.checkpoint_id
                       where (checkpoint_reportables.frequency_id in (1,13))
                       and checkpoint_reportables.id in {self.helper.resolve_ids_for_query(self.request['reportables'])}
                        and checkpoint_reportables.start_report_date <= '{self.request['date']} 00:00:00'
                        and enabled = true
                        {self.type_query()}
                       group by checkpoint_reportables.id, authorized_flow
                   """
        else:
            sql = f"""
                       select checkpoint_reportables.*,
                       caf.authorized_flow as authorized_flow  from 
                       checkpoint_reportables 
                        left join (select distinct(checkpoint_id), authorized_flow from checkpoint_authorized_flows) as caf on caf.checkpoint_id = checkpoint_reportables.checkpoint_id
                       where (checkpoint_reportables.frequency_id in (1,13))
                       and checkpoint_reportables.start_report_date <=  '{self.request['date']} 00:00:00'
                        and enabled = true
                        {self.type_query()}
                       group by checkpoint_reportables.id, authorized_flow
                   """
            # redeploy
        return self.all(sql)

    def get_extended_reportables(self):
        frequency_id = self.get_frequency_id(self.request['extended'])
        if frequency_id is not None:
            sql = f"""
                       select checkpoint_reportables.*,
                       caf.authorized_flow as authorized_flow  from 
                       checkpoint_reportables 
                        left join (select distinct(checkpoint_id), authorized_flow from checkpoint_authorized_flows) as caf on caf.checkpoint_id = checkpoint_reportables.checkpoint_id
                       where checkpoint_reportables.frequency_id = {frequency_id}
                       {self.type_query()}
                       and checkpoint_reportables.start_report_date <=  '{self.request['date']} 00:00:00'
                        and enabled = true
                       group by checkpoint_reportables.id, authorized_flow
                   """
            return self.all(sql)
        else:
            return []

    def type_query(self):
        if 'type' in self.request:
            return f" and checkpoint_reportables.type_id in (select id from reportable_types where name = '{self.request['type']}')"
        return ""

    def get_frequency_id(self, slug):
        sql = f"""
            select * from frequencies where slug = '{slug}'
        """
        frequencies = self.all(sql)
        if len(frequencies) > 0:
            self.frequency = frequencies[0]
            return frequencies[0]['id']
        return None

    def get_sensors(self):
        if 'reportables' in self.request:
            sql = f"""
                select * from reportable_sensors 
                where reportable_id in {self.helper.resolve_ids_for_query(self.request['reportables'])}
            """
        else:
            sql = f"""
                select * from reportable_sensors
            """
        return self.all(sql)

    def get_reports_sent(self, request):
        if 'extended' in request:
            if 'reportables' in request:
                sql = f"""
                   select 
                       id, reportable_id, uuid, verification_response_code, report_date 
                   from 
                       checkpoint_reports 
                   where 
                       report_date between '{request['date']} 00:00:00' and '{request['date']} 23:59:59' 
                       and (verification_response_code::int = 0 or verification_response_code::int = 60 or response_code::int = 0)
                       and reportable_id in {self.helper.resolve_ids_for_query(request['reportables'])}
                       order by report_date asc    
                """
            else:
                sql = f"""
                       select 
                           id, reportable_id, uuid, verification_response_code, report_date 
                       from 
                           checkpoint_reports 
                       where 
                           report_date between '{request['date']} 00:00:00' and '{request['date']} 23:59:59' 
                           and (verification_response_code::int = 0 or verification_response_code::int = 60 or response_code::int = 0)
                           order by report_date asc    
                    """
        else:
            if 'reportables' in request:
                sql = f"""
                                select 
                                    id, reportable_id, uuid, verification_response_code, report_date 
                                from 
                                    checkpoint_reports 
                                where 
                                    report_date between '{request['date']} 00:00:00' and '{request['date']} 23:59:59' 
                                    and (verification_response_code::int = 0 or verification_response_code::int = 60 or response_code::int = 0)
                                    and reportable_id in {self.helper.resolve_ids_for_query(request['reportables'])}
                                    order by report_date asc    
                            """
            else:
                sql = f"""
                                select 
                                    id, reportable_id, uuid, verification_response_code, report_date 
                                from 
                                    checkpoint_reports 
                                where 
                                    report_date between '{request['date']} 00:00:00' and '{request['date']} 23:59:59' 
                                    and (verification_response_code::int = 0 or verification_response_code::int = 60 or response_code::int = 0)
                                    order by report_date asc    
                            """
        return self.all(sql)

    def calculate_extended_date(self):
        end_date = datetime.strptime(datetime.strptime(
            self.request['date'], "%Y-%m-%d"
        ).strftime("%Y-%m-%d 23:59:59"), "%Y-%m-%d %H:%M:%S")
        if self.frequency['id'] == 3:
            return {
                "end_date": end_date.strftime("%Y-%m-%d"),
                "start_date": self.csd(end_date).strftime("%Y-%m-%d")
            }
        else:
            return {
                "end_date": end_date.strftime("%Y-%m-%d"),
                "start_date": self.csd(end_date).strftime("%Y-%m-01")
            }

    def csd(self, date):
        return date - timedelta(hours=((self.frequency['in_seconds']) / 3600))

    @staticmethod
    def second_to_days(seconds):
        return seconds / 3600 / 24

    def get_reportables_with_sensors_and_reports(self, request):
        reportables = self.get_reportables()
        reports = self.get_reports_sent(request)
        sensors = self.get_sensors()
        frequencies = self.get_frequencies()
        for reportable in reportables:
            reportable['sensors'] = list(filter(lambda row: row['reportable_id'] == reportable['id'], sensors))
            reportable['reports_sent'] = list(filter(lambda row: row['reportable_id'] == reportable['id'], reports))
            frequency = list(filter(lambda row: row['id'] == reportable['frequency_id'], frequencies))[0]
            reportable['frequency_in_seconds'] = frequency['in_seconds']
            if reportable['frequency_id'] == 1:
                reportable['must_report_count'] = 24
            else:
                reportable['must_report_count'] = 1
            reportable['sent_reports_count'] = len(reportable['reports_sent'])
            reportable['missing_reports'] = self.calculate_missing_reports(reportable, request)
        return reportables

    def get_frequencies(self):
        sql = "select * from frequencies"
        return self.all(sql)

    def get_clients(self):
        return self.all("select * from clients")
