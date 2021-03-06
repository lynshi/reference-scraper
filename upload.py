import os

from refscrap.uploader import Uploader


def upload():
    uploader = Uploader('player_dict.json')
    wd = os.getcwd() + '/'
    data_dir = wd + 'data/basketball/'

    for file in os.listdir(data_dir):
        if file.find('.csv') == -1 or file.find('advanced') != -1:
            continue
        file = data_dir + file

        player_id = Uploader.get_player_id(file)
        player_key = uploader.add_player('NBA', player_id)

        if player_key is None:
            continue

        uploader.add_player_stats(file, player_key, player_id)


if __name__ == '__main__':
    upload()
