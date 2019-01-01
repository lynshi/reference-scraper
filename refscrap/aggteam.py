import pandas as pd
from pandas.errors import EmptyDataError
import os

from refscrap.uploader import Uploader


def create_aggregated_data_frame(directory):
    df = None
    for f in os.listdir(directory):
        if f.find('.csv') == -1 or f.find('advanced') == -1:
            continue
        file = directory + f
        print(file)
        if df is None:
            df = pd.read_csv(file)
        else:
            try:
                df = df.append(pd.read_csv(file))
            except pd.errors.EmptyDataError:
                print('empty file: ' + file)
                exit()

    def get_season(row):
        season = int(row['Date'].split('-')[0])
        month = int(row['Date'].split('-')[1])
        if month > 7:
            season += 1
        return season

    df['Season'] = df.apply(get_season, axis=1)

    return df


def get_game_logs(df, uploader: Uploader):
    for team in ['SAS', 'NOP', 'PHO', 'POR', 'OKC', 'LAL', 'DAL', 'DEN', 'TOR',
                 'BOS', 'MIL', 'IND', 'MIN', 'ATL', 'HOU', 'LAC', 'UTA', 'MEM',
                 'NYK', 'BRK', 'DET', 'CLE', 'SAC', 'MIA', 'ORL', 'CHO', 'CHI',
                 'PHI', 'WAS']:
        get_game_logs_for_team(df, team, uploader)


def get_game_logs_for_team(df, team, uploader: Uploader):
    seasons = [2017, 2018, 2019]
    games = [i for i in range(1,83)]

    for season in seasons:
        for game in games:
            game_df = df[(df['Season'] == season) &
                         (df['Rk'] == game) &
                         (df['Tm'] == team)]
            print(game_df)
            exit()
