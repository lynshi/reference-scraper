from bs4 import BeautifulSoup
import csv
import json
import os
import random
import requests
import time


class BasketballReferenceScraper:
    DATA_STAT_TO_IGNORE = {
        'game_location', 'game_result'
    }

    def __init__(self, mu=10, sigma=1):
        self.data_out_dir = os.getcwd() + '/data/basketball/'
        random.seed()
        self.mu = mu
        self.sigma = sigma
        self.last_break_time = time.clock()
        self.consecutive_fails = 0

    @property
    def _url(self):
        return 'https://www.basketball-reference.com'

    @property
    def _player_index_url(self):
        return self._url + '/players'

    def scrape(self):
        for letter in 'abcdefghijklmnopqrstuvwxyz':
            self.scrape_player_index(letter)

    def query(self, url, timeout=5):
        if time.clock() - self.last_break_time >= random.gauss(900, 15):
            time.sleep(round(random.gauss(300, 15)))
            self.last_break_time = time.clock()
        else:
            time.sleep(round(random.gauss(self.mu, self.sigma), 3))
        for i in range(10):
            try:
                response = requests.get(url, timeout=timeout)
                break
            except requests.exceptions.ReadTimeout as e:
                time.sleep(random.randint(1,5))
                if i == 9:
                    raise RuntimeError('too many timeouts')
        if response.status_code != 200 and response.status_code != 404:
            self.consecutive_fails += 1
            if self.consecutive_fails >= 10:
                raise RuntimeError('10 consecutive request errors')
        else:
            self.consecutive_fails = 0
        return response

    def scrape_player_index(self, first_letter):
        url = self._player_index_url + '/' + first_letter

        page_response = self.query(url, timeout=5)
        if page_response.status_code != 200:
            return

        page_content = BeautifulSoup(page_response.content, 'html.parser')
        table_div = page_content.find('div', {'id': 'all_players'})
        player_table = table_div.findChildren('table')[0]
        rows = player_table.findChildren('tr')[1:]

        for row in rows:
            strong = row.findChildren('strong')
            if len(strong) == 0:
                continue

            suffix = row.findChildren('a')[0]['href']
            self.scrape_player(self._url + suffix)

    def generate_player_dict_entry(self, url):
        if os.path.isfile('player_dict.json') is True:
            with open('player_dict.json') as infile:
                player_dict = json.load(infile)
        else:
            player_dict = {}

        player_id = url.split('/')[-1].split('.')[0]
        if player_id in player_dict:
            return

        player_dict[player_id] = {'Id': player_id}
        resp = self.query(url, timeout=5)
        if resp.status_code != 200:
            return
        
        content = BeautifulSoup(resp.content, 'html.parser')
        name = content.find('h1', {'itemprop': 'name'}).string
        player_dict[player_id]['Name'] = name

        # do not get position because multiple positions may be listed and 
        # consistency with DFS rosters is not guaranteed

        ps = content.findChildren('p')
        idx = None
        for i, p in enumerate(ps):
            if p.getText().find('Team:') != -1:
                idx = i
                break        
        if idx is not None:
            team_link = ps[idx].find('a')['href']
            team = team_link.split('/')[2]
            player_dict[player_id]['Team'] = team

        with open('player_dict.json', 'w') as outfile:
            outfile.write(json.dumps(player_dict, indent=4))

    def scrape_player(self, url):
        self.generate_player_dict_entry(url)
        base_url = url[:-5]
        if self.download_game_logs(base_url, 2017, 2019) is False:
            return
        self.download_advanced_game_logs(base_url, 2017, 2019)
        used_space = sum(os.path.getsize(f) for f in os.listdir(
            self.data_out_dir) if os.path.isfile(f))

        if used_space > 7000000000:
            raise RuntimeError('Too much space (' + str(used_space) +
                               ') bytes used')

    def download_advanced_game_logs(self, base_url, start_year, end_year,
                                    skip_existing=True):
        csv_out_name = \
            os.path.join(self.data_out_dir, base_url.split('/')[-1] +
                         '-advanced.csv')
        if skip_existing is True and os.path.isfile(csv_out_name) is True:
            return

        csv_headings = []
        csv_rows = []

        current = start_year
        while current <= end_year:
            year = str(current)
            current += 1
            resp = self.query(base_url + '/gamelog-advanced/' + year,
                              timeout=5)
            if resp.status_code != 200:
                continue

            content = BeautifulSoup(resp.content, 'html.parser')
            div = content.find('div', {'id': 'all_pgl_advanced'})
            if div is None:
                continue

            if len(csv_headings) == 0:
                thead = div.find('thead')
                ths = thead.findChildren('th')
                for th in ths:
                    if th['data-stat'] in \
                            BasketballReferenceScraper.DATA_STAT_TO_IGNORE:
                        continue
                    csv_headings.append(th.string)

            trs = div.findChildren('tr')[1:]
            for tr in trs:
                row_content = [len(csv_rows) % 82 + 1]
                tds = tr.findChildren('td')
                append = (len(tds) > 0)
                for td in tds:
                    if td['data-stat'] in \
                            BasketballReferenceScraper.DATA_STAT_TO_IGNORE:
                        continue
                    elif td['data-stat'] == 'reason':
                        while len(row_content) < len(csv_headings):
                            row_content.append(0)
                        break

                    row_content.append(td.string)

                    if td['data-stat'] == 'age':
                        row_content[-1] = row_content[-1].split('-')[0]

                if append is True:
                    csv_rows.append(row_content)

        with open(csv_out_name, 'w') as outfile:
            csv_writer = csv.writer(outfile)
            csv_writer.writerow(csv_headings)
            csv_writer.writerows(csv_rows)

    def download_game_logs(self, base_url, start_year, end_year,
                           skip_existing=True):
        csv_out_name = \
            os.path.join(self.data_out_dir, base_url.split('/')[-1] + '.csv')
        if skip_existing is True and os.path.isfile(csv_out_name) is True:
            return False

        csv_headings = []
        csv_rows = []

        current = start_year
        while current <= end_year:
            year = str(current)
            current += 1
            resp = self.query(base_url + '/gamelog/' + year, timeout=5)
            if resp.status_code != 200:
                continue

            content = BeautifulSoup(resp.content, 'html.parser')
            div = content.find('div', {'id': 'all_pgl_basic'})
            if div is None:
                continue

            if len(csv_headings) == 0:
                thead = div.find('thead')
                ths = thead.findChildren('th')
                for th in ths:
                    if th['data-stat'] in \
                            BasketballReferenceScraper.DATA_STAT_TO_IGNORE:
                        continue
                    csv_headings.append(th.string)

            if len(csv_headings) != 28:
                csv_headings = []
                continue

            trs = div.findChildren('tr')[1:]
            for tr in trs:
                row_content = [len(csv_rows) % 82 + 1]
                tds = tr.findChildren('td')
                append = (len(tds) > 0)
                for td in tds:
                    if td['data-stat'] in \
                            BasketballReferenceScraper.DATA_STAT_TO_IGNORE:
                        continue
                    elif td['data-stat'] == 'reason':
                        while len(row_content) < len(csv_headings):
                            row_content.append(0)
                        break

                    row_content.append(td.string)

                    if td['data-stat'] == 'age':
                        row_content[-1] = row_content[-1].split('-')[0]

                if append is True:
                    csv_rows.append(row_content)

        if len(csv_headings) == 0:
            return False

        with open(csv_out_name, 'w') as outfile:
            csv_writer = csv.writer(outfile)
            csv_writer.writerow(csv_headings)
            csv_writer.writerows(csv_rows)

        return True
