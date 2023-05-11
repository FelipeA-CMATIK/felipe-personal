import json
import base64
from verifier import Verifier


def make_verify(event, context):
    if 'data' in event:
        message = base64.b64decode(event['data']).decode('utf-8')
        if message == 'verify-reports':
            verifier = Verifier()
            verifier.verify_reports()
