from database import Database
from datetime import datetime
from datetime import timedelta
from bigquery import BigQuery
from task import Task
import json
from helper import Helper
from frequency import Frequency
import requests


class Reportable(Database):
    def __init__(self, frequency):
        super().__init__()
        self.helper = Helper()
        self.frequency = self.get_frequency(frequency)
        self.bigQuery = BigQuery()
        self.start_date = self.helper.fix_timezone(datetime.now()).strftime("%Y-%m-%d %H:00:00")
        self.fixed_start_date = self.helper.fix_timezone(datetime.now() - timedelta(hours=self.calculate_hours(self.frequency))).strftime("%Y-%m-%d %H:00:00")
        self.end_date = self.helper.fix_timezone(datetime.now()).strftime("%Y-%m-%d %H:05:00")
        self.clients = self.get_clients()
        self.columns = [
            'uuid',
            'reportable_sensor_id',
            'reported_value',
            'read_value',
            'read_date'
        ]
        self.table = "checkpoint_reported_sensors"
        self.task = Task()

    @staticmethod
    def calculate_hours(frequency):
        return frequency['in_seconds'] / 3600

    def send_reports(self):
        reportables = self.get_reportables_with_sensors()
        if len(reportables) > 0:
            data = []
            for reportable in reportables:
                self.helper.print(reportable, 1)
                try:
                    inserted = self.insert_report(reportable)

                    for sensor in reportable['sensors']:
                        data.append({
                            'uuid': inserted,
                            'reportable_sensor_id': sensor['id'],
                            'reported_value': None,
                            'read_value': sensor['report_value'],
                            'read_date': sensor['report_date'],
                        })

                    reportable['uuid'] = inserted
                    self.task.create_task_on_queue(reportable)
                    self.helper.print(json.dumps(reportable, default=str), 2)
                except Exception as e:
                    self.helper.print(e, 0)
            self.insert_sensors(data)
            self.helper.print("Reportes enviados a task", 1)
        else:
            self.helper.print("No hay puntos de control a reportar", 1)
    
    def insert_sensors(self, sensors_data):
        self.execute_with_payload(
                self.massive_insert_statement(len(sensors_data)),
                self.flatten(sensors_data)
            )  
        self.helper.print("sensores insertados", 1)

    def insert_report(self, reportable):
        uuid = self.helper.generate_uuid(str(reportable['id']) + self.helper.fix_timezone(datetime.now()).strftime('%Y%m%d%H%M%S'))
        sql = f"""
                INSERT INTO checkpoint_reports (
                    reportable_id,
                    uuid,
                    report_date,
                    sync,
                    created_at      
                ) VALUES (
                    {reportable['id']},
                    '{uuid}',
                    '{self.start_date}',
                    0,
                    '{self.helper.fix_timezone(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')}'
                )
        """
        self.execute(sql)
        return uuid

    def get_reportables_with_sensors(self):
        reportables = self.filter_reportables(self.get_reportables_by_frequency())
        if len(reportables) > 0:
            sensors = self.get_sensors_by_reportables(self.helper.pluck(reportables, 'id'))
            last_reports = self.get_last_report_from_sensors(self.helper.pluck(reportables, 'id'))
            types = self.get_reportable_types()

            for reportable in reportables:
                for type_field in reportable['type_fields'].keys():
                    reportable[type_field] = reportable['type_fields'][type_field]
                reportable['type'] = list(filter(lambda row: row['id'] == reportable['type_id'], types))
                reportable['report_date'] = datetime.strptime(self.start_date,'%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
                reportable['report_time'] = datetime.strptime(self.start_date,'%Y-%m-%d %H:%M:%S').strftime('%H:%M:%S')
                reportable['sensors'] = list(filter(lambda row: row['reportable_id'] == reportable['id'], sensors))
                reportable['frequency_in_seconds'] = self.frequency['in_seconds']

                if reportable['type'][0]['authentication_url'] is not None:
                    client = list(filter(lambda row: row['id'] == reportable['client_id'], self.clients))[0]
                    if client['last_reportable_token'] is None or self.calculate_time_of_token(client):
                        reportable = self.get_token_for_client(reportable)
                    else:
                        reportable['last_token_used'] = client['last_reportable_token']
                        reportable['token_expiration'] = client['last_reportable_token_expiration']

                for sensor in reportable['sensors']:
                    last_report = list(filter(lambda row: row['reportable_sensor_id'] == sensor['id'], last_reports))
                    if last_report and sensor['last_report_value'] is None and last_report[0]['read_value'] < sensor['last_report_value']:
                        last_report_value = last_report[0]['read_value']
                        last_read = last_report[0]['read_date']
                    else:
                        if sensor['last_report_value'] != '':
                            last_report_value = sensor['last_report_value']
                            last_read = sensor['last_report_date']
                        else:
                            last_report_value = 0,
                            last_read = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    if sensor['turn_to_positive']:
                        last_report_value = last_report_value * -1
                    if sensor['decimals'] == 0:
                        last_report_value = int(last_report_value)
                    else:
                        if last_report_value != '':
                            last_report_value = '{:.{decimals}f}'.format(float(last_report_value), decimals=int(sensor['decimals']))
                        else:
                            last_report_value = 0
                    sensor['last_report_value'] = last_report_value
                    sensor['last_report_date'] = last_read
        return reportables

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
            UPDATE
                clients 
            SET 
                last_reportable_token = '{content['token']}', 
                last_reportable_token_expiration = '{content['expiration']}'
            WHERE
             id = {reportable['client_id']}
            """)
        self.clients = self.get_clients()
        return reportable

    def calculate_time_of_token(self, client):
        date = client['last_reportable_token_expiration']
        return ((date - datetime.now()).total_seconds()) < 120

    def get_last_report_from_sensors(self, reportables):
        sql = f"""
            SELECT 
                crs.*,
                cr.reportable_id,
                cr.sync 
            FROM 
                checkpoint_reported_sensors crs
                INNER JOIN (
                    SELECT 
                        p1.uuid,
                        p1.reportable_id, 
                        p1.sync
                    FROM 
                        checkpoint_reports p1 
                        LEFT JOIN checkpoint_reports p2 ON (p1.reportable_id = p2.reportable_id AND p1.id < p2.id)
                    WHERE p2.id IS NULL AND p1.reportable_id IN {self.helper.resolve_ids_for_query(reportables)}) 
                AS cr ON cr.uuid = crs.uuid;
        """
        return self.all(sql)

    def get_reportable_types(self):
        sql = f"""
            SELECT
                * 
            FROM
                reportable_types
        """
        return self.all(sql)

    def filter_reportables(self, reportables):
        reps = []
        for reportable in reportables:
            if reportable['last_report_date'] is None:
                reps.append(reportable)
            else:
                first_date = datetime.strptime(self.helper.fix_timezone(datetime.now()).strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
                freq = Frequency(self.frequency)
                if reportable['last_sync'] == 2 or freq.check_frequency(reportable['last_report_date'],first_date):
                    reps.append(reportable)
        return reps
  
    def get_frequency(self, frequency):
        sql = f"select * from frequencies where slug = '{frequency}'"
        return self.first(sql)

    def get_reportables_by_frequency(self):
        query = f"""
            SELECT 
            checkpoint_reportables.*,
            caf.authorized_flow as authorized_flow,
            cr.sync as last_sync,
            clients.last_reportable_token,
            clients.last_reportable_token_expiration
            FROM checkpoint_reportables 
            left join clients on clients.id = checkpoint_reportables.client_id
            left join (SELECT p1.sync, p1.reportable_id
            FROM checkpoint_reports p1 LEFT JOIN checkpoint_reports p2
              ON (p1.reportable_id = p2.reportable_id AND p1.id < p2.id)
            WHERE p2.id IS NULL ) 
            as cr on cr.reportable_id = checkpoint_reportables.id
            left join (select distinct(checkpoint_id), authorized_flow from checkpoint_authorized_flows) as caf on caf.checkpoint_id = checkpoint_reportables.checkpoint_id
            where frequency_id = {self.frequency['id']}
            and checkpoint_reportables.type_id = 2            
            and enabled = true
            and start_report_date <= '{self.helper.fix_timezone(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")}' 
            and  (end_report_date is null or end_report_date >= '{self.helper.fix_timezone(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")}')
        """
        return self.all(query)    

    def get_sensors_by_reportables(self,ids):
        query = f"""
            SELECT * FROM reportable_sensors WHERE reportable_id in {self.helper.resolve_ids_for_query(ids)}
        """ 
        sensors = self.all(query)
        old_sensors = self.get_old_sensors(self.helper.pluck(sensors,'sensor_id'))
        for sensor in sensors:
            sensor['old_id'] = list(filter(lambda row:row['id'] == sensor['sensor_id'], old_sensors))[0]['id']

        rows = self.bigQuery.get_rows(self.helper.pluck(sensors,'old_id'),self.fixed_start_date, self.end_date)
        sensors_data = [dict(row) for row in rows]
        for sensor in sensors:
            data = list(filter(lambda row: row['sensor_id'] == sensor['old_id'], sensors_data))
            if data:
                value = data[-1]['result']
                sensor['last_report_value'] = data[0]['result']
                sensor['report_value'] = value
                sensor['report_date'] = data[-1]['date']
                sensor['last_report_date'] = data[0]['date']
                sensor['report_id_bq'] = data[-1]['id']
                sensor['last_report_id_bq'] = data[0]['id']
            else:
                sensor['report_value'] = None
                sensor['report_date'] = None
        return sensors   

    def get_old_sensors(self,sensor_ids):
        query = f"""
            select * from sensors where id in {self.helper.resolve_ids_for_query(sensor_ids)}
        """
        return self.all(query)

    def get_clients(self):
        return self.all("select * from clients")