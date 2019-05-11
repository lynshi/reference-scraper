# reference-scraper

Code to scrape *-reference.com for seasonal athlete statistics, which are then aggregated to form seasonal team statistics. Currently, there is only code for basketball-reference.com.

To prevent TOS violations from scraping, there is a 10 second<sup>1, 2</sup> interval between requests, and 5 minute breaks<sup>1</sup> every 15 minutes<sup>1</sup>.

<sup>1</sup> The actual number is normally distributed around this mean to appear more random.

<sup>2</sup> This corresponds to roughly the amount of time it took me to load a page and download statistics as a CSV.