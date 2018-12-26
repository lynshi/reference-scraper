from bs4 import BeautifulSoup
import requests


class BasketballReferenceScraper:
    @property
    def _url(self):
        return 'https://www.basketball-reference.com/'

    @property
    def _player_index_url(self):
        return self._url + 'players/'

    def scrape(self):
        for letter in 'abcdefghijklmnopqrstuvwxyz':
            self.scrape_player_index(letter)

    def scrape_player_index(self, first_letter):
        if first_letter != 'a' and first_letter != 'x':
            return
        url = self._player_index_url + first_letter
        page_response = requests.get(url, timeout=5)
        if page_response.status_code != 200:
            return

        page_content = BeautifulSoup(page_response.content, "html.parser")
