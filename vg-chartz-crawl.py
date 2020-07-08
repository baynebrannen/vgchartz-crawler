import pandas as pd
import numpy as np
import datetime
from datetime import date
import re
import math
import scrapy
import json
import logging
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from timeit import default_timer as timer

# Here is our JSON writer pipeline

class JsonWriterPipeline(object):

    # When the spider is open, it writes itself to the gamesresult to a julia file
    def open_spider(self, spider):
        self.file = open('gamesresult.jl', 'w')

    # When the spider closes, it closes that file as it is done writing to it
    def close_spider(self, spider):
        self.file.close()

    # This function dictates how the spider writes to the .jl file
    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line)
        return item

# I'll just create a string for today's date to append to the end of the names of the files we create
today_date_string = str(date.today().month) + "_" + str(date.today().day) + "_" + str(date.today().year)

page = 2 # The first page we will need to jump to is page number 2, so this is that variable
genre = 0 # We are starting at the first genre in the list

# The quick and admittedly dirty way to do this is if we want to know the genre of each game is to cycle through the urls with
# each of the following genres as a way of changing the genre parameter of the web address

genre_list = ["Action",
             "Adventure",
             "Action-Adventure",
             "Board+Game",
             "Education",
             "Fighting",
             "Misc",
             "MMO",
             "Music",
             "Party",
             "Platform",
             "Puzzle",
             "Racing",
             "Role-Playing",
             "Sandbox",
             "Shooter",
             "Simulation",
             "Sports",
             "Strategy",
             "Visual+Novel"]

class VGSpider(scrapy.Spider):
    global genre

    name = "game_spider"
    start_urls = ['https://www.vgchartz.com/games/games.php?name=&keyword=&console=&region=All&developer=&publisher=&goty_year=&genre=' + genre_list[0] + '&boxart=Both&banner=Both&ownership=Both&showmultiplat=No&results=200&order=Sales&showtotalsales=0&showtotalsales=1&showpublisher=0&showpublisher=1&showvgchartzscore=0&showvgchartzscore=1&shownasales=0&shownasales=1&showdeveloper=0&showdeveloper=1&showcriticscore=0&showcriticscore=1&showpalsales=0&showpalsales=1&showreleasedate=0&showreleasedate=1&showuserscore=0&showuserscore=1&showjapansales=0&showjapansales=1&showlastupdate=0&showlastupdate=1&showothersales=0&showothersales=1&showshipped=0&showshipped=1']

    # Here's where we set the logging settings.
    custom_settings = {
        'LOG_LEVEL': logging.WARNING,
        'ITEM_PIPELINES': {'__main__.JsonWriterPipeline': 1}, # This uses the functions in the JsonWriterPipeline class to write the
                                                              # Julia file
        'FEED_FORMAT':'json',                                 # This sets the feed exporter to export as a JSON file
        'FEED_URI': "gamesresult-" + today_date_string + ".json" # This simply sets the title for said JSON file
    }

    def parse(self, response):

        global genre
        global page

        # Here we declare the selector for each piece of data--this tells scrapy where to check for each item

        IMAGE_SELECTOR = './/td[2]/div/a/div/img/@src'
        TITLE_SELECTOR = './/td[3]/a/text()'
        CONSOLE_SELECTOR = './/td[4]/img/@alt'
        PUBLISHER_SELECTOR = './/td[5]/text()'
        DEVELOPER_SELECTOR = './/td[6]/text()'
        VGSCORE_SELECTOR = './/td[7]/text()'
        CRITIC_SELECTOR = './/td[8]/text()'
        USER_SELECTOR = './/td[9]/text()'
        TOTALSHIPPED_SELECTOR = './/td[10]/text()'
        TOTALSALES_SELECTOR = './/td[11]/text()'
        NASALES_SELECTOR = './/td[12]/text()'
        PALSALES_SELECTOR = './/td[13]/text()'
        JPSALES_SELECTOR = './/td[14]/text()'
        OTHER_SELECTOR = './/td[15]/text()'
        RELEASEDATE_SELECTOR = './/td[16]/text()'
        UPDATE_SELECTOR = './/td[17]/text()'

        # We loop through each row (so each game) in the table, and snag the data we want, giving each one a name that make sense

        for row in response.xpath('//*[@id="generalBody"]/table[1]/tr'):
            yield {

                'img' : row.xpath(IMAGE_SELECTOR).extract(),
                'title' : row.xpath(TITLE_SELECTOR).extract(),
                'console' : row.xpath(CONSOLE_SELECTOR).extract(),
                'publisher' : row.xpath(PUBLISHER_SELECTOR).extract(),
                'developer' : row.xpath(DEVELOPER_SELECTOR).extract(),
                'vg_score' : row.xpath(VGSCORE_SELECTOR).extract(),
                'critic_score' : row.xpath(CRITIC_SELECTOR).extract(),
                'user_score' : row.xpath(USER_SELECTOR).extract(),
                'total_shipped' : row.xpath(TOTALSHIPPED_SELECTOR).extract(),
                'total_sales' : row.xpath(TOTALSALES_SELECTOR).extract(),
                'na_sales' : row.xpath(NASALES_SELECTOR).extract(),
                'pal_sales' : row.xpath(PALSALES_SELECTOR).extract(),
                'jp_sales' : row.xpath(JPSALES_SELECTOR).extract(),
                'other_sales' : row.xpath(OTHER_SELECTOR).extract(),
                'release_date' : row.xpath(RELEASEDATE_SELECTOR).extract(),
                'last_update' : row.xpath(UPDATE_SELECTOR).extract(),
                'genre' : genre_list[genre]
            }

        # next_page puts together--you guessed it--the next page. It uses the page number we established at the beginning
        # and the genre parameter we're on currently.

        next_page = "https://www.vgchartz.com/games/games.php?page=" + str(page) + "&results=200&name=&console=&keyword=&publisher=&genre="+ genre_list[genre] + "&order=Sales&ownership=Both&boxart=Both&banner=Both&showdeleted=&region=All&goty_year=&developer=&direction=DESC&showtotalsales=1&shownasales=1&showpalsales=1&showjapansales=1&showothersales=1&showpublisher=1&showdeveloper=1&showreleasedate=1&showlastupdate=1&showvgchartzscore=1&showcriticscore=1&showuserscore=1&showshipped=1&alphasort=&showmultiplat=No"

        # This selector will help us know just how far to go by grabbing the number of results for the given query total, not
        # just on the page

        RESULTS_SELECTOR = '//*[@id="generalBody"]/table[1]/tr[1]/th[1]/text()'

        # Below we grab that figure which is a string, and extract the numbers from it using a regular expression

        results = response.xpath(RESULTS_SELECTOR).extract_first()
        results_pat = r'([0-9]{1,9})'
        results = results.replace(",", "")

        # We then can divide that number by 200 (the number of results per page), and thusly figure out just how many pages we
        # should cycle through before we've reached the end and should either switch genres or finish up.

        last_page = math.ceil(int(re.search(results_pat, results).group(1)) / 200)

        # Below we test whether we are at the last page and whether we've reached the last page of the last genre
        # If we've reached the last page and we've reached the last genre (which according to our list is Visual Novel)
        # we can end out spider and close up shop.

        if (page > last_page) & (genre_list[genre] == "Visual+Novel"):
            print(genre_list[genre])
            return "All done!"

        # If we've reached the last page, but not the last genre (anything BUT Visual Novel), we'll reset our page, move onto the
        # next genre and keep scraping.
        elif (page > last_page) & (genre_list[genre] != "Visual+Novel"):
            print(genre_list[genre])
            page = 1
            genre += 1
            next_page = "https://www.vgchartz.com/games/games.php?page=" + str(page) + "&results=200&name=&console=&keyword=&publisher=&genre="+ genre_list[genre] + "&order=Sales&ownership=Both&boxart=Both&banner=Both&showdeleted=&region=All&goty_year=&developer=&direction=DESC&showtotalsales=1&shownasales=1&showpalsales=1&showjapansales=1&showothersales=1&showpublisher=1&showdeveloper=1&showreleasedate=1&showlastupdate=1&showvgchartzscore=1&showcriticscore=1&showuserscore=1&showshipped=1&alphasort=&showmultiplat=No"
            yield scrapy.Request(
                response.urljoin(next_page),
                callback=self.parse
                )
            page += 1

        # If we haven't reached the last page at all, we can just keep going without changing the genre parameter, we'll just have
        # to move onto the next page
        elif next_page:
            yield scrapy.Request(
                response.urljoin(next_page),
                callback=self.parse
                )
            page += 1

# Here we finally simply create the process and set the spider we've created to crawl away
if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    start = timer()
    process.crawl(VGSpider)
    process.start(stop_after_crawl=True) # Blocks here until the crawl is finished
    end = timer()
    print("It took " + str(end - start) + " seconds to retrieve this data.")

    # And then we can read in the JSON file we've created

    games = pd.read_json("gamesresult-" + today_date_string + ".json")

    # Now we'll clean it up!
    # The webcrawler pulled three blank rows for each page because of the structure of the table, so let's go ahead
    # and filter those out. The crawler also pulled everything as lists with a single element due to the JSON, so
    # we'll be checking the length of each element to see if there's anything inside, if not we'll toss it.

    games = games[~(games["title"].str.len() == 0)]

    # Next we'll need to convert our single-element lists to the elements themselves, so we'll select the first element
    # in each list to be the actual value we want in each cell.

    for column in ['console', 'critic_score', 'developer', 'img', 'jp_sales',
           'last_update', 'na_sales', 'other_sales', 'pal_sales', 'publisher',
           'release_date', 'title', 'total_sales', 'total_shipped', 'user_score',
           'vg_score']:
        games[column] = games[column].apply(lambda x : x[0])

    # There are also some trailing spaces on some of the columns, so let's go ahead and trim those off with str.strip()

    games = games.apply(lambda x : x.str.strip())

    # If we hadn't done the previous step, this wouldn't have worked, but now we can convert all "N/A" strings in the
    # dataset to numpy nan values.

    games = games.replace("N/A", np.nan)

    # Now we'll start cleaning the individual columns. I'll first write a function to clean each numerical column, since
    # they'll all follow the same general rules.

    def clean_nums(column, dataframe):
        dataframe[column] = dataframe[column].str.strip("m") # This will strip the "m" off the end of each string if it's there
        dataframe[column] = dataframe[column].apply(lambda x : float(x)) # This will turn all the values from strings to floats

    # Now we'll just apply it to all of the sales columns

    sales_columns = ["na_sales",
                     "jp_sales",
                     "pal_sales",
                     "other_sales",
                     "total_sales",
                     "total_shipped",
                     "vg_score",
                     "user_score",
                     "critic_score"]

    for column in sales_columns:
        clean_nums(column, games)

    # Next we'll need to clean up the dates which are in string format. We'll use some regex to extract the information
    # and then convert to datetime objects.

    day_pat = r"([0-9]{2})(?=[a-z]{2})" # A regex pattern to select only the numerical day of the month
    month_pat = r"([A-Z][a-z]{2})" # A regex pattern to select only the string abbreviated month
    year_pat = r"([0-9]{2}(?![a-z]{2}))" # A regex pattern to select only the numerical year

    # And here is a month string to month integer translation dictionary.

    month_to_num = {'Sep' : 9,
                    'Jul' : 7,
                    'Oct' : 10,
                    'Mar' : 3,
                    'Dec' : 12,
                    'Feb' : 2,
                    'Nov' : 11,
                    'Jun' : 6,
                    'Aug' : 8,
                    'May' : 5,
                    'Apr' : 4,
                    'Jan' : 1
                    }

    # Now we're actually ready to create the function to clean the dates.

    def clean_dates(text):
        global day_pat
        global month_pat
        global year_pat
        global month_to_num
        if text is np.nan:
            return text

        day = int((re.search(day_pat, text).group(1))) # This one is the easiest, we extract the numbers and convert to integer
        month = month_to_num[(re.search(month_pat, text).group(1))] # This one we use the month string to look up the integer in our dictionary
        year = (re.search(year_pat, text).group(1)) # Year will require a little more work as we need to fill in the first two digits

        # The best we can do here is see how high the first integer is, and if it's greater than seven, we can safely assume
        # (for now) that it was from the nineties. Otherwise, it's in the 2000s. That will be alright before 2070 in which case
        # we've got a classic y2k situation ready to ruin us.

        if int(year[0]) >= 7:
            year = int("19" + year)
        else:
            year = int("20" + year)

        return(datetime.datetime(year, month, day))

    # We will apply the date cleanup function across our two date columns.

    for column in ["last_update", "release_date"]:
        games[column] = games[column].apply(lambda x : clean_dates(x))

    # A quick replacement of the +'s used in the url for genre to make our genre column more readable

    games["genre"] = games["genre"].str.replace("+", " ")

    # In exploring the data, I noticed there were a few dates being used
    # as placeholders. For the longevity of this code, I will not exclude
    # 12/31/2020 and 12/31/2021, but any user using this code before these dates
    # should be careful to not rely on them, and explore to see if they are
    # still being used as placeholders. However, 1/1/1970 is used as a
    # placeholder, and is the only date to show up in 1970, thus I can replace
    # these dates with a nan value while keeping our mind at ease.

    games.loc[games["release_date"].dt.year == 1970, "release_date"] = np.nan 

    # Finally, let's reorder the columns in a way that makes the most sense.

    games = games[["img", "title", "console", "genre", "publisher", "developer", "vg_score", "critic_score", "user_score", "total_shipped", "total_sales", "na_sales", "jp_sales", "pal_sales", "other_sales", "release_date", "last_update"]]

    # And now we have a delightful little clean dataframe of videogames that's as current as possible!

    games.to_csv("vgchartz-" + today_date_string +".csv", index=False)

    print(str(games["title"].count()) + " game records retrieved." )
    print("File saved as vgchartz-" + today_date_string + ".csv")
