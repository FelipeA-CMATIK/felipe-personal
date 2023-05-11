from database import Database
from helper import Helper
import json


class Report(Database):
    def __init__(self):
        super().__init__()
        self.helper = Helper()

    def update_report(self, request):
        sql = f"""
            update 
                checkpoint_reports 
            set 
                verification_code = '{request['verification_code']}',
                response_code = '{request['response_code']}',
                response_message = '{request['response_message']}',
                reported_at = '{request['reported_at']}',
                sync = {request['sync']},
                reported_body_json = '{json.dumps(request)}',
                reported_body = '{request['reported_body']}',
                uri_used = '{request['uri_used']}'
            where
                uuid = '{request['uuid']}'
        """
        self.helper.print(sql, 2)
        self.execute(sql)

        for sensor in request['reported_body_json']['callback_data']['sensors']:
            sensorSql = f"""
                UPDATE
                    checkpoint_reported_sensors
                SET
                    reported_value = {request['reported_body_json'][sensor['link_to']]},
                    recalculated = {request['reported_body_json']['callback_data']['recalculable']}
                WHERE
                    uuid = '{request['uuid']}'
                    and id = {sensor['id']}  
            """
            self.helper.print(sensorSql, 2)
            self.execute(sensorSql)
        if request['sync'] == 1:
            self.set_last_report_date(request)

    def set_last_report_date(self, request):
        sql = f"""
            SELECT 
                c.* 
            FROM 
                checkpoint_reports c
            WHERE 
                c.uuid = '{request['uuid']}'
        """
        reportable_id = self.first(sql)
        reportable_id = reportable_id['reportable_id']

        sql2 = f"""
            UPDATE
                checkpoint_reportables 
            SET 
                last_report_date = '{request['reported_body_json']['report_date']} {request['reported_body_json']['report_time']}',
                last_recalculation = '{request['reported_at']}',
                last_report_status = {int(request['response_code'])}
            WHERE
                id = {reportable_id}
        """
        self.execute(sql2)
