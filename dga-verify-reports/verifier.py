import os
from database import Database
import json
from task import Task


class Verifier(Database):
    def __init__(self):
        super().__init__()
        self.task = Task()

    def verify_reports(self):
        reports = self.get_reports()
        for report in reports:
            payload = {
                'uuid': report['uuid'],
                'verification_code': report['verification_code'],
                'work_code': report['reportable'][0]['type_fields']['work_code'],
                'callback': os.environ.get('CALLBACK_URL')
            }
            self.task.create_task_on_queue(payload)
            print(json.dumps(payload, default=str))

    def get_reports(self):
        reports = self.get_reports_data()
        reportables = self.get_reportables()
        for report in reports:
            report['reportable'] = list(filter(lambda row: row['id'] == report['reportable_id'], reportables))

        return reports

    def get_reportables(self):
        sql = f"""
            select * from checkpoint_reportables 
        """
        return self.all(sql)

    def get_reports_data(self):
        sql = f"""
            select * from checkpoint_reports where completed_at is null and verification_code is not null
        """
        return self.all(sql)
