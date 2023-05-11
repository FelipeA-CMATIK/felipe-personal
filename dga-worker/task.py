from google.cloud import tasks_v2
import os
import json
from helper import Helper


class Task:
    def __init__(self, type):
        self.queue = os.environ.get('QUEUE_ID')
        self.cloudFunction = 'https://us-central1-cmatik-pro-resources.cloudfunctions.net/dga-service'
        self.method = 'POST'
        self.project = os.environ.get('PROJECT_ID')
        self.location = 'us-central1'
        self.client = tasks_v2.CloudTasksClient()
        self.path = self.client.queue_path(self.project, location=self.location, queue=self.queue)
        self.helper = Helper()

    def create_task_on_queue(self, payload):
        task = {
            'http_request': {
                'http_method': tasks_v2.HttpMethod.POST,
                'url': self.cloudFunction,
                "oidc_token": {
                    "service_account_email": os.environ.get('TASK_SERVICE_ACCOUNT')
                }
            }
        }  
        self.helper.print(task, 1)

        if payload is not None:
            self.helper.print(payload, 1)
            if isinstance(payload, dict):
                payload = json.dumps(payload)
                task["http_request"]["headers"] = {"Content-type": "application/json"}
            converted_payload = payload.encode()
            task["http_request"]["body"] = converted_payload

        self.client.create_task(parent=self.path, task=task)  
    