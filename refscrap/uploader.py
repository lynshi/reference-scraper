from google.cloud import datastore

from csv import DictReader
import json


class Uploader:
    def __init__(self, player_dict_json):
        self.client = datastore.Client.from_service_account_json(
            '~/.api_credentials/gcloud_datastore.json')
        with open(player_dict_json) as infile:
            self.player_dict = json.load(infile)

    @staticmethod
    def get_player_id(csv_file_name):
        return csv_file_name.split('/')[-1].split('.')[0]

    def add_player(self, league, player_id):
        key = self.client.key(league, player_id)
        player = datastore.Entity(key)
        d =
        player.update()

    def add_player_stats(self):
        pass

    def add_player_advanced_stats(self):
        pass
