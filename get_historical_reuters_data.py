# Dependencies

# built-ins
import os # making and reading directories
import time # for wait functions
import multiprocessing # get how many CPUs are in your PC

# 3rd-party
import pandas as pd # for data processing and .csv I/O 
from tqdm import tqdm # for progress bars
from newspaper import Article # for parsing Reuters articles 
from selenium import webdriver # for web scraping
from datetime import datetime # for getting today's date
from joblib import Parallel, delayed # for parallel processing

# Functions
def get_data_for_stock(stock):
	# get_data_for_stock()

	# Takes input "stock" and outputs a {stock}.csv file to the reuters_data directory.
	# Also, a folder called "processed" contains more folders whose names are of the
	# stocks that have already been processed. This is for making it easy to just run the 
	# script again if it is stopped (e.g. your PC crashes, you have to kill the script)

	# Input: stock (str) - ticker symbol of a designated stock
	# Output: None

	# Set the Firefox webdriver to run headless in the background
	fireFoxOptions = webdriver.FirefoxOptions() 
	fireFoxOptions.set_headless() 
	
	driver = webdriver.Firefox(options = fireFoxOptions) # Initialize the webdriver instance in the background

	try:
		# Search Reuters for {stock}
		driver.get('https://www.reuters.com/search/news?blob={}'.format(stock)) 
		time.sleep(2) # Wait for the page to load

		# Reuters should query the company if they have written articles on it.
		# If they haven't, an error will be thrown and no files will be outputted
		# This line gets the company they queried's element which contains
		# the stock's name and URL to Reuters's page on the stock
		text = driver.find_element_by_xpath('/html/body/div[4]/section[2]/div/div[1]/div[3]/div/div/div/div[1]/a').text

		# {condition} will determine if the stock queried is actually 
		# the stock that we're trying to get articles on
		condition = False

		# Reuters will format the element's text in 2 ways: 
		
		# {company name} ({ticker}.{some additional text})
		# e.g. 
		# Apple Inc (AAPL.OQ)
		# or
		# {company name} ({ticker})
		# e.g.
		# Alcoa Corp (AA)

		# The following lines of code will filter down the element's text
		# to just get {ticker}
		# e.g. 
		# Apple Inc (AAPL.OQ) --> AAPL
		# Aloca Corp (AA)     --> AA
		if '.' in text: # Check if a period is in the text
			# Check if {ticker}.upper() == {stock}.upper()
			if text[text.find('(') + 1:text.find('.')].upper() == stock.upper(): 
				condition = True # {condition} = True means that the queried stock is a match
		else: # If there is no period 
			# Check if {ticker}.upper() == {stock}.upper()
			if text[text.find('(') + 1: text.find(')')].upper() == stock.upper():
				condition = True # {condition} = True means that the queried stock is a match

		if condition: # If {stock} has been found in Reuters, continue

			# Click the element's link, going to Reuters's 
 			driver.find_element_by_xpath('/html/body/div[4]/section[2]/div/div[1]/div[3]/div/div/div/div[1]/a').click()
			time.sleep(0.5) # Let the stock's Reuters page load

			# Go to the "News" section of the stock's Reuters page
			driver.find_element_by_xpath('/html/body/div[1]/div/div[3]/div/div/nav/div[1]/div/div/ul/li[2]/button').click()
			time.sleep(5)

			# The next segment will scroll down to the bottom of the "News"
			# page of the stock's Reuters page
			SCROLL_PAUSE_TIME = 0.5 # This is how much time it waits before scrolling to 
									# the bottom of the page again

			# Get the last height of the page
			last_height = driver.execute_script("return document.body.scrollHeight")
			
			while True:
				# Scroll to the bottom of the page
				driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

				# Wait for more content to load 
				time.sleep(SCROLL_PAUSE_TIME)

				# Get the current height of the page
				new_height = driver.execute_script("return document.body.scrollHeight")
				if new_height == last_height: 
					# If the current height of the page is the same as it was before,
					# break the script because there is no more content to load.
					break

				last_height = new_height # Get the previous height of the page
			i = 1 # Article index number starts at one in the HTML
			datas = [] # Put all of the data in here
			tol = 0 # Tol is the amount of times an error has been thrown (for this stock news page).
					# When it hits 3, it's confirmed that all articles for this stock
					# have been queried.

			# The amount of articles on the page is unknown, so a while loop is
			# used to iterate until no more new articles are found
			while True:
				try:
					# This is the xpath for the title of the article
					xpath = '/html/body/div[1]/div/div[4]/div[1]/div/div/div/div[2]/div[{}]/div/a'.format(i)
					i += 1
					# An error will be thrown if there are no more articles left because
					# the driver won't be able to find the non-existent next article.

					header = driver.find_element_by_xpath(xpath).text # Get the article's header text
					link = driver.find_element_by_xpath(xpath).get_attribute('href') # Get the link of the article
					
					# The links are processed by a seperate script because
					# less threads have to be devoted to 

					datas.append([header, link])
				except Exception as e:
					tol += 1 # Increase the error tally by 1 
					time.sleep(30) 

					if tol > 2: # The script will only break if 3 errors in a row are thrown to confirm
								# it's actually found all the articles
						break

			datas = pd.DataFrame(datas, columns = ['text', 'link']) # Compile the list of headers and links into a pandas DataFrame
			datas.to_csv('reuters_data/{}.csv'.format(stock)) # Export the data to the reuters data folder under the name {stock}.csv

		# Stop the driver
		driver.quit()
	except Exception as e:	
		time.sleep(30) # Make this worker wait a bit before killing incase Reuters.com
					   # is acting up
		driver.quit() # The webdriver will be killed upon receiving an error
					  # to save space on RAM

def convert_link_to_data(link):
	# convert_link_to_data()

	# Given a Reuters article link, it will parse the article for authors, 
	# the article's publish date, and the article's content.

	# Input: link (str) - link to a designated Reuters article
	# Output: authors (list of strs), date article was published (pd.Timestamp, UTC+0), 
	#         and full, raw article content

	try:
		article = Article(link) # Instantiate the Article() object 
		article.download() # Download the article
		article.parse() # Parse the article for data
		authors = article.authors # Get the article's authors
		publish_date = article.publish_date # Get the date the article was published on
		text = article.text # Get the article's main text
		return [authors, publish_date, text]

	except Exception as e:
		# If the article isn't found, an error will be thrown and 
		# NaNs will be outputted.
		return [np.nan, np.nan, np.nan] 

# Load all NYSE stocks
nyse_listed = pd.read_csv('nyse-listed_csv.csv', index_col = 0).reset_index()
# Load all NASDAQ stocks
nasdaq_listed = pd.read_csv('nasdaq-listed-symbols_csv.csv', index_col = 0).reset_index()
nasdaq_listed.columns = ['ACT Symbol', 'Company Name']
# Append the NASDAQ stock list to the NYSE stock list
nyse_listed = nyse_listed.append(nasdaq_listed)
# Load all US stocks that aren't on the NYSE or NASDAQ
other_listed = pd.read_csv('other-listed_csv.csv', index_col = 0).reset_index()
other_listed = other_listed[['ACT Symbol', 'Company Name']]
# Append the other stock list to the NYSE + NASDAQ stock list
nyse_listed = nyse_listed.append(other_listed)
# Get all unique ticker symbols from the NYSE + NASDAQ + Other stock list
symbols = nyse_listed['ACT Symbol'].unique().tolist()
all_stocks = [] # All stocks will be the stocks that are going to be processed

if 'processed' not in os.listdir(): # If there is no "processed" directory, create one
	print('"processed" was not found in the current working directory.')
	print('Creating it...')
	os.mkdir('processed')
	print('Done')
	print('\n')
else:
	print('"processed" was found in your current working directory.')
	print('\n')

if 'reuters_data' not in os.listdir(): # If there is no "reuters_data" directory, create one
	print('"reuters_data" was not found in the current working directory.')
	print('Creating it...')
	os.mkdir('reuters_data')
	print('Done')
	print('\n')
else:
	print('"reuters_data" was found in your current working directory.')
	print('\n')

for stock in symbols:
	try:
		# If the stock hasn't been processed yet, add it to {all_stocks}
		# {all_stocks} are all stocks that haven't been processed yet and
		# are going to be
		if (stock + '.csv' not in os.listdir('reuters_data')) & (stock not in os.listdir('processed')):
			all_stocks.append(stock) # Add the stock to the list
	except:
		pass


cpu_count = multiprocessing.cpu_count() # get number of CPU cores

bp = False

print('NOTE: If you do not want to wait ~16+ hours to get the whole dataset, just download it from kaggle at https://www.kaggle.com/miguelaenlle/reuters-articles-for-3500-stocks-since-2017')
print('\n')

print('{} cores were found on your system.'.format(cpu_count))

while True:
	if bp:
		break
	num_cores_to_use = input('How many threads would you like to allocate to scraping (leave empty for maximum)?: ')
	try:
		if len(num_cores_to_use) > 0:
			num_cores_to_use = int(num_cores_to_use)
			if num_cores_to_use >= 1:
				if num_cores_to_use <= 16:
					print('Using {} threads to scrape.'.format(num_cores_to_use))
					bp = True
					break
				else:
					print('Warning: {} threads has not been tested yet (16 was used for the Kaggle data scrape)'.format(num_cores_to_use))
					while True:
						confirm = input('Would you like to proceed (y/n)?: ')
						confirm = confirm.lower()
						confirm = confirm.replace(' ', '')
						if confirm == 'y':
							print('Using {} threads to scrape.'.format(num_cores_to_use))
							bp = True
							break
						elif confirm == 'n':
							print('Okay,')
							bp = False
							continue
						else:
							print("Please type 'Y' or 'N'. Caps doesn't matter.")


		else:
			num_cores_to_use = cpu_count
			if num_cores_to_use <= 16:
				print('Using {} threads to scrape.'.format(num_cores_to_use))
				bp = True
				break
			else:
				print('Warning: {} threads has not been tested yet (16 was used for the Kaggle data scrape)'.format(num_cores_to_use))
				while True:
					confirm = input('Would you like to proceed (y/n)?: ')
					confirm = confirm.lower()
					confirm = confirm.replace(' ', '')
					if confirm == 'y':
						print('Using {} threads to scrape.'.format(num_cores_to_use))
						bp = True
						break
					elif confirm == 'n':
						print('Okay,')
						bp = False
						continue
					else:
						print("Please type 'Y' or 'N'. Caps doesn't matter.")
		
	except:
		print('Please input an integer.')

num_scraper_cores = num_cores_to_use

while True:
	if bp:
		break
	num_cores_to_use = input('How many threads would you like to allocate to parsing articles (leave empty for maximum)?: ')
	try:
		if len(num_cores_to_use) > 0:
			num_cores_to_use = int(num_cores_to_use)
			if num_cores_to_use >= 1:
				if num_cores_to_use <= 4:
					print('Using {} threads to scrape.'.format(num_cores_to_use))
					bp = True
					break
				else:
					print('Warning: {} threads has not been tested yet (4 was used for the Kaggle data scrape)'.format(num_cores_to_use))
					while True:
						confirm = input('Would you like to proceed (y/n)?: ')
						confirm = confirm.lower()
						confirm = confirm.replace(' ', '')
						if confirm == 'y':
							print('Using {} threads to scrape.'.format(num_cores_to_use))
							bp = True
							break
						elif confirm == 'n':
							print('Okay,')
							bp = False
							continue
						else:
							print("Please type 'Y' or 'N'. Caps doesn't matter.")


		else:
			num_cores_to_use = cpu_count
			if num_cores_to_use <= 4:
				print('Using {} threads to scrape.'.format(num_cores_to_use))
				bp = True
				break
			else:
				print('Warning: {} threads has not been tested yet (4 was used for the Kaggle data scrape)'.format(num_cores_to_use))
				while True:
					confirm = input('Would you like to proceed (y/n)?: ')
					confirm = confirm.lower()
					confirm = confirm.replace(' ', '')
					if confirm == 'y':
						print('Using {} threads to scrape.'.format(num_cores_to_use))
						bp = True
						break
					elif confirm == 'n':
						print('Okay,')
						bp = False
						continue
					else:
						print("Please type 'Y' or 'N'. Caps doesn't matter.")
		
	except:
		print('Please input an integer or nothing.')

num_parser_cores = num_cores_to_use
print('Setting verbosity to high is recommended to make sure your script is still operational.')
while True:
	
	verbosity = input('Set verbosity (high, medium, low, off): ')
	verbosity = verbosity.replace(' ', '')
	verbosity = verbosity.lower()
	if verbosity.lower() == 'high':
		print('Setting verbosity to HIGH')
		verbosity = 20
		break
	elif verbosity.lower() == 'medium':
		print('Setting verbosity to MEDIUM')
		verbosity = 5
		break
	elif verbosity.lower() == 'low':
		print('Setting verbosity to LOW')
		verbosity = 1
		break
	elif verbosity.lower() == 'off':
		confirm = input('Are you sure? You will not be able to see script progress (y/n)...: ')
		confirm = confirm.lower()
		confirm = confirm.replace(' ', '')
		if confirm == 'y':
			print('Okay, turning off output.')
			verbosity = 0
			break
		else:
			print('Select a verbosity level...')
			continue
	else:
		print("""Please type "high", "medium", "low", or "off". Caps doesn't matter.""")
		continue
# Run the scraper with the designated amount of cores.

while True:
	confirm = input("Do you want to scrape Reuters? Type 'n' if you've already scraped. (y/n): ")
	confirm = confirm.lower()
	confirm = confirm.replace(' ', '')
	if confirm == 'y':
		print('Scraping all Reuters news articles. This ~12 hours to run on 16 threads.')
		Parallel(num_scraper_cores, 'loky', verbose = 20)(delayed(get_data_for_stock)(stock) for stock in all_stocks)
	elif confirm == 'n':
		print('Ok, skipping.')
	else:
		print("""Please type "y" or "n". Caps doesn't matter.""")
		continue

print('All reuters articles have been scraped. Downloading into a pandas Dataframe...')
datas = [] 
for file in tqdm(os.listdir('reuters_data')):
    path = 'reuters_data/{}'.format(file)
    data = pd.read_csv(path, index_col = 0)
    if len(data) > 0:
        data['stock'] = file[:-4]
        datas.append(data)

datas = pd.concat(datas)
datas = datas.drop_duplicates(keep = 'first').dropna().reset_index(drop = True)
links = datas['link']
datas.columns = ['header'] + datas.columns[1:].tolist()
while True:
	confirm = input("Do you want to parse downloaded articles? Type 'n' if you have already scraped it. (y/n): ")
	confirm = confirm.lower()
	confirm = confirm.replace(' ', '')
	if confirm == 'y':
		print('Parsing all scraped articles. This takes ~4-5 hours to run on 4 threads.')
		reuters_processed = Parallel(num_parser_cores, 'loky', verbose = 20)(delayed(get_data_for_stock)(stock) for stock in all_stocks)
		reuters_processed.to_csv('reuters_processed2.csv')
	elif confirm == 'n':
		print('Ok, skipping.')
	else:
		print("""Please type "y" or "n". Caps doesn't matter.""")
		continue

print('Data mining is complete. Processing data into a usable format.')

from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA

reuters_processed = pd.read_csv('reuters_processed2.csv', index_col = 0)
datas.columns = reuters_processed
sentiments = []
for i in tqdm(datas.index):
    sentiments.append(SIA().polarity_scores(datas.loc[i, 'header']))
sentiments = pd.DataFrame(sentiments)
datas[sentiments.columns] = sentiments
datas.columns = ['raw_header', 'reuters_url', 'stock', 'article_publish_date', 'full_article', 'processed_header', 'neg_sentiment', 'neu_sentiment', 'pos_sentiment', 'compound_sentiment']
datas.to_csv('reuters_data.csv')