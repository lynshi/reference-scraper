from google.cloud import datastore

from csv import DictReader
import json


class Uploader:
    def __init__(self, player_dict_json):
        self.client = datastore.Client.from_service_account_json(
            '/home/lynshi/.api_credentials/gcloud_datastore.json')
        with open(player_dict_json) as infile:
            self.player_dict = json.load(infile)

    @staticmethod
    def get_player_id(csv_file_name):
        return csv_file_name.split('/')[-1].split('.')[0]

    def add_player(self, league, player_id):
        key = self.client.key(league, player_id)
        player = datastore.Entity(key)
        player.update(self.player_dict[player_id])
        self.client.put(player)

        return player.key

    def add_player_stats(self, csv_file_name, player_key):
        game_logs = {}
        with open(csv_file_name) as infile:
            stat_reader = DictReader(infile)
            for row in stat_reader:
                tup = (row['Date'].split('-')[0], row['Rk'])
                d = {}
                for col in row:
                    if len(row[col]) == 0:
                        d[col] = 'nan'
                    elif col in {'Tm', 'Opp', 'MP', 'Date'}:
                        d[col] = row[col]
                    elif col.find('%') != -1 or col == 'GmSc':
                        d[col] = float(row[col])
                    else:
                        d[col] = int(row[col])
                game_logs[tup] = d
                break

        with open(csv_file_name.replace('.', '-advanced.')) as infile:
            stat_reader = DictReader(infile)
            for row in stat_reader:
                tup = (row['Date'].split('-')[0], row['Rk'])
                d = {}
                for col in row:
                    if col in {'Tm', 'Opp', 'MP', 'Date'}:
                        d[col] = row[col]
                    elif col.find('%') != -1 or col == 'GmSc':
                        d[col] = float(row[col])
                    else:
                        d[col] = int(row[col])
                game_logs[tup].update(d)
                break

        
