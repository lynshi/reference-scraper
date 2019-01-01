from refscrap.aggteam import *


def aggregate_and_upload():
    df = create_aggregated_data_frame('data/basketball/')
    uploader = Uploader('player_dict_cp.json')
    get_game_logs(df, uploader)


if __name__ == '__main__':
    aggregate_and_upload()
