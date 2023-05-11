from database import Database
from zoneinfo import ZoneInfo
import os
from datetime import datetime
import json


class Report(Database):
    def __init__(self):
        super().__init__()

    def close_verification(self, request):
        if int(request['verification_code']) == 61:
            sync = "sync = 0"
        else:
            sync = "sync = sync"
        sql = f"""
            update
                checkpoint_reports
            set
                verified = true,
                verification_response_code = '{request['verification_code']}',
                verification_message = '{request['verification_message']}',
                completed_at = '{self.fix_timezone(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')}',
                verification_response = '{json.dumps(request)}',
                {sync}
            where
                uuid = '{request['uuid']}'         
        """
        self.execute(sql)

    def fix_timezone(self, date):
        return date.astimezone(ZoneInfo(os.environ.get('TIMEZONE')))