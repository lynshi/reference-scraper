from google.cloud import datastore

from csv import DictReader
import json


class Uploader:
    def __init__(self, player_dict_json):
        self.client = datastore.Client.from_service_account_json(
            '../.api_credentials/gcloud-datastore.json')
        with open(player_dict_json) as infile:
            self.player_dict = json.load(infile)

        self.team_dict = {
            'SAS': 'San Antonio Spurs', 'NOP': 'New Orleans Pelicans',
            'PHO': 'Phoenix Suns', 'POR': 'Portland Trail Blazers',
            'OKC': 'Oklahoma Thunder', 'LAL': 'Los Angeles Lakers',
            'DAL': 'Dallas Mavericks', 'DEN': 'Denver Nuggets',
            'TOR': 'Toronto Raptors', 'BOS': 'Boston Celtics',
            'MIL': 'Milwaukee Bucks', 'IND': 'Indianapolis Pacers',
            'MIN': 'Minnesota Timberwolves', 'ATL': 'Atlanta Hawks',
            'HOU': 'Houston Rockets', 'LAC': 'Los Angeles Clippers',
            'UTA': 'Utah Jazz', 'MEM': 'Memphis Grizzlies',
            'NYK': 'New York Knicks', 'BRK': 'Brooklyn Nets',
            'DET': 'Detroit Pistons', 'CLE': 'Cleveland Cavaliers',
            'SAC': 'Sacremento Kings', 'MIA': 'Miami Heat',
            'ORL': 'Orlando Magic', 'CHO': 'Charlotte Hornets',
            'CHI': 'Chicago Bulls', 'PHI': 'Philadelphia 76ers',
            'WAS': 'Washington Wizards'
        }

    @staticmethod
    def get_player_id(csv_file_name):
        return csv_file_name.split('/')[-1].split('.')[0]

    def add_player(self, league, player_id):
        if player_id not in self.player_dict:
            return None

        key = self.client.key(league, player_id)
        player = datastore.Entity(key)
        player.update(self.player_dict[player_id])
        self.client.put(player)

        return player.key

    def add_team(self, league, team):
        key = self.client.key(league, team)
        team_entity = datastore.Entity(key)
        team_entity.update({'Name': self.team_dict[team], 'Id': team,
                            'Team': team})
        self.client.put(team_entity)

        return team_entity.key

    def add_team_game_logs(self, team_key, logs):
        items = []
        for tup in logs.keys():
            season = str(tup[0])
            game = tup[1]
            stats = logs[tup]
            key = self.client.key(season, game, parent=team_key)
            item = datastore.Entity(key, exclude_from_indexes=(
                'FG', 'FGA', '3P', '3PA',
                'FT', 'FTA', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK',
                'TOV', 'PF', 'PTS'
            ))
            item.update(stats)
            items.append(item)

        self.client.put_multi(items)

    def add_player_stats(self, csv_file_name, player_key, player_id):
        game_logs = {}
        with open(csv_file_name) as infile:
            stat_reader = DictReader(infile)
            for row in stat_reader:
                season = int(row['Date'].split('-')[0])
                month = int(row['Date'].split('-')[1])
                if month > 7:
                    season += 1
                tup = (str(season), int(row['Rk']))
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

        with open(csv_file_name.replace('.', '-advanced.')) as infile:
            stat_reader = DictReader(infile)
            for row in stat_reader:
                season = int(row['Date'].split('-')[0])
                month = int(row['Date'].split('-')[1])
                if month > 7:
                    season += 1
                tup = (str(season), int(row['Rk']))
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
                try:
                    game_logs[tup].update(d)
                except KeyError:
                    raise RuntimeError(str(tup) + ' missing in ' +
                                       csv_file_name)

        items = []
        for tup in game_logs.keys():
            season = tup[0]
            game = tup[1]
            stats = game_logs[tup]
            stats['Name'] = self.player_dict[player_id]['Name']
            key = self.client.key(season, game, parent=player_key)
            item = datastore.Entity(key, exclude_from_indexes=(
                'Age', 'GS', 'MP', 'FG', 'FGA', 'FG %', '3P', '3PA', '3P %',
                'FT', 'FTA', 'FT %', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK',
                'TOV', 'PF', 'PTS', 'GmSc', '+ / -',
                'TS%', 'eFG%', 'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%', 'BLK%',
                'TOV%', 'USG%', 'ORtg', 'DRtg'
            ))
            item.update(stats)
            items.append(item)

        self.client.put_multi(items)
