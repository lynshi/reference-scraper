from refscrap.uploader import Uploader


def upload():
    uploader = Uploader('player_dict.json')

    uploader.add_player_stats('data/basketball/abrinal01.csv', None)


if __name__ == '__main__':
    upload()
