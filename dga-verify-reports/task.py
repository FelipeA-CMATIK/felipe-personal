from google.cloud import tasks_v2
import os
import json


class Task:
    def __init__(self):
        self.location = 'us-central1'
        self.project = os.environ.get('PROJECT_ID')
        self.queue = os.environ.get('QUEUE_ID')
        self.cloudFunction = 'dga-verify'
        self.method = 'POST'
        # define client
        self.client = tasks_v2.CloudTasksClient()
        self.path = self.client.queue_path(self.project, location=self.location, queue=self.queue)

    def create_task_on_queue(self, payload):
        task = {
            'http_request': {  # Specify the type of request.
                'http_method': tasks_v2.HttpMethod.POST,
                'url': f"https://{self.location}-{self.project}.cloudfunctions.net/{self.cloudFunction}",
                "oidc_token": {
                    "service_account_email": os.environ.get('TASK_SERVICE_ACCOUNT')
                }
            }
        }

        if payload is not None:
            if isinstance(payload, dict):
                payload = json.dumps(payload, default=str)
                task["http_request"]["headers"] = {"Content-type": "application/json"}
            converted_payload = payload.encode()
            task["http_request"]["body"] = converted_payload
        print(payload)
        self.client.create_task(parent=self.path, task=task)
