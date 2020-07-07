# Reuters Scraping Tools

These are the scripts used for the getting the dataset shown here: https://www.kaggle.com/miguelaenlle/reuters-articles-for-3500-stocks-since-2017

## Getting Started

### Prerequisites

All the prerequisite files are in requirements.txt
Also, install selenium webdriver firefox.

### Installing

1. Download this repository
2. Install the requirements via $ pip install -r requirements.txt

## Usage

reuters_scraper.py is for getting scrape data for individual stocks.
reuters_full_database_scraper.py is for scraping the entire Reuters database.

To use reuters_scraper.py, 
- Put your script in this directory
- import reuters_scraper

reuters_scraper has two functions:
- get_data_for_stock
- get_data_for_stock_with_lookback

**get_data_for_stock** takes 2 arguments: stock and verbose
If verbose is False, get_data_for_stock will not print anything.
**get_data_for_stock** gets the entire reuters article history for the stock.
Use this for backtesting.

**get_data_for_stock_with_lookback** also takes 2 arguments: stock and lookback
It behaves similarly to get_data_for_stock, but lookback controls the amount of articles you want.
Lookback is the amount of days backward the earliest publish date for an article should be, e.g. 7 day lookback = Get articles for the past 7 days

To use reuters_full_database_scraper.py, simply call the script via python reuters_full_database_scraper.py and follow the prompts shown on the
command line.

