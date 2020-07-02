
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
def get_data_for_stock(stock, verbose = False, massive_scrape_mode = False):
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
	if massive_scrape_mode:
		if stock.replace('.', '_') not in os.listdir('processed'): 
			os.mkdir('processed/{}'.format(stock.replace('.', '_')))  # Instantiate a folder whose name is {stock}
													# to mark that this stock has already been processed 

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
			if verbose:
				print('Stock was found on Reuters.')
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
			it_num = 0
			if verbose:
				print('Scrolling to the bottom of the news page...')
			while True:
				if verbose:
					if it_num % 10 == 0:
						print('{} - Scroll: Iteration #{}'.format(stock, it_num))
					it_num += 1
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
			if verbose:
				print('Scroll completed.')
			i = 1 # Article index number starts at one in the HTML
			datas = [] # Put all of the data in here
			tol = 0 # Tol is the amount of times an error has been thrown (for this stock news page).
					# When it hits 3, it's confirmed that all articles for this stock
					# have been queried.

			# The amount of articles on the page is unknown, so a while loop is
			# used to iterate until no more new articles are found
			if verbose:
				print('Scraping the site...')
			it_num = 0
			while True:
				try:
					if verbose:
						if it_num % 10 == 0:
							print('{} - Scrape: Iteration #{}'.format(stock, it_num))

						it_num += 1
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

					# print(datas)
					if tol >= 2: # The script will only break if 3 errors in a row are thrown to confirm
								# it's actually found all the articles
						break
			
			datas = pd.DataFrame(datas, columns = ['text', 'link']) # Compile the list of headers and links into a pandas DataFrame
			if verbose == False:
				print('Scraping for further information....')
			links_data = Parallel(1, 'threading', verbose = 0)(delayed(convert_link_to_data)(link) for link in datas['link'].values.tolist())
			links_data = pd.DataFrame(links_data, columns = ['author', 'publish_date', 'body_text'])


			if massive_scrape_mode == True:
				links_data.to_csv('reuters_data/{}.csv'.format(stock.replace('.', '_'))) # Export the data to the reuters data folder under the name {stock}.csv
			else:
				pass
		else:
			if verbose:
				print('Stock not found on reuters.')
		# Stop the driver
		driver.quit()
		try:
			if massive_scrape_mode == False:
				return links_data
			
		except:
			if massive_scrape_mode == False:
				return False
			
	except Exception as e:	
		print(e)
		time.sleep(30) # Make this worker wait a bit before killing incase Reuters.com
					   # is acting up
		driver.quit() # The webdriver will be killed upon receiving an error
					  # to save space on RAM
# get_data_for_stock('ABT')
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


data_for_stock = get_data_for_stock('NNVC', True)
print(data_for_stock)