import json
import base64
from reportable import Reportable


def make_reports(event, context):

    if 'data' in event:
        message = base64.b64decode(event['data']).decode('utf-8')
        if message == 'make-reports-hourly':
            reportable = Reportable('hourly')
        elif message == 'make-reports-daily':
            reportable = Reportable('daily')
        elif message == 'make-reports-monthly':
            reportable = Reportable('monthly')
        elif message == 'make-reports-semi-annually':
            reportable = Reportable('semi-annually')
        elif message == 'make-reports-annually':
            reportable = Reportable('annually')

        reportable.send_reports()
