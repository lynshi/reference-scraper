from google.cloud import datastore


class Uploader:
    def __init__(self):
        self.client = datastore.Client.from_service_account_json(
            '~/.api_credentials/gcloud_datastore.json')
