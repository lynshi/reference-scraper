from bs4 import BeautifulSoup
import csv
import os
import random
import requests
import time


class BasketballReferenceScraper:
    def __init__(self, mu, sigma):
        self.data_out_dir = os.getcwd() + '/data/basketball/'
        random.seed()
        self.mu = mu
        self.sigma = sigma

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
        response = requests.get(url, timeout=timeout)
        time.sleep(random.gauss(self.mu, self.sigma))
        return response

    def scrape_player_index(self, first_letter):
        if first_letter != 'a' and first_letter != 'x':
            return
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

    def scrape_player(self, url):
        base_url = url[:-5]
        self.download_game_log(base_url, '2014', '2019')

    def download_game_log(self, base_url, start_year, end_year):
        start = int(start_year)
        end = int(end_year)
        data_stat_to_ignore = {
            'ranker', 'game_location', 'game_result',
            'game_score', 'plus_minus'
        }
        csv_headings = []
        csv_rows = []

        while start <= end:
            year = str(start)
            start += 1
            resp = self.query(base_url + '/gamelog/' + year, timeout=5)
            if resp.status_code != 200:
                continue

            content = BeautifulSoup(resp.content)
            div = content.find('div', {'id': 'all_pgl_basic'})

            if len(csv_headings) == 0:
                thead = div.find('thead')
                ths = thead.findChildren('th')
                for th in ths:
                    if th['data-stat'] in data_stat_to_ignore:
                        continue
                    csv_headings.append(th.string)

            trs = div.findChildren('tr')[1:]
            for tr in trs:
                row_content = []
                tds = tr.findChildren('td')
                for td in tds:
                    if td['data-stat'] in data_stat_to_ignore:
                        continue
                    row_content.append(td.string)

                    if td['data-stat'] == 'age':
                        row_content[-1] = row_content[-1].split('-')[0]

                csv_rows.append(row_content)

        csv_out_name = self.data_out_dir + base_url.split('/')[-1]
        with open(csv_out_name, 'w') as outfile:
            csv_writer = csv.writer(outfile)
            csv_writer.writerow(csv_headings)
            csv_writer.writerows(csv_rows)

        exit()
