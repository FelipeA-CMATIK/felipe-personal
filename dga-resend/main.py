import json
import base64
from report import Report


def resend_reports(event, context):

    if 'data' in event:
        message = base64.b64decode(event['data']).decode('utf-8')
        if message == 'resend_reports':
            report = Report()
            report.resend_reports()



