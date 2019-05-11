# reference-scraper

Code to scrape *-reference.com for seasonal athlete statistics, which are then aggregated to form seasonal team statistics. Currently, there is only code for basketball-reference.com.

To prevent TOS violations from scraping, there is a 10 second[^1][^2] interval between requests, and 5 minute breaks[^1] every 15[^1] minutes.

[^1]: The actual number is normally distributed around this mean to appear more random.

[^2]: This corresponds to roughly the amount of time it took me to load a page and download statistics as a CSV.