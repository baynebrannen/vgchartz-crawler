# vgchartz-crawler
This project uses scrapy to crawl vgchartz.com and pull data on every videogame that they have made available from their database.

It will create a JSON file, and then clean and export that file as a CSV using pandas. 
The filename will be vgchartz-{month_date_year}

I was playing around with a dataset posted on Kaggle by Github user ashaheedq (https://www.kaggle.com/ashaheedq/video-games-sales-2019), but the questions I wanted answered required release date information that his crawler did not pull. I decided to build my own crawler and scrape the website myself to get the data I needed. Mine is different in a few ways. His goes through each game individually on the website whereas mine goes through the tableview of the database and pulls the data that way. Mine takes significantly less time (less than 10 minutes, but I was not able to pull ESRB ratings (or their international equivalents). This didn't matter much to me, so it was worth it to me to have a much quicker scraper and to end up with more data than his scraper pulled. While I certainly drew inspiration from his project in concept (scraping vgchartz itself), I did not draw inspiration from his code and instead created mine with the help of a scrapy tutorial and the scrapy documentation:

https://www.digitalocean.com/community/tutorials/how-to-crawl-a-web-page-with-scrapy-and-python-3
https://docs.scrapy.org/en/latest/topics/item-pipeline.html


There are certainly many improvements that could be made to this project. Per the above, each game could be looked at individually in order to get the absolute maximum information about each title rather than in the tableview of the website; this would inevitably result in a longer processing period. I'm also think that multi-threading and proxies could speed up the process. The way that it selects the genre parameter may also be somewhat messy--it may not be sustainable since the available genres may change throughout the years.

IMPORTANT: Please note that vgchartz.com sometimes uses impossible placeholder dates as the release dates for certain titles--not neccesarily titles in the future that they don't
have the release date for yet, but oddly enough for titles from the past for which they have not collected the correct release date. I have ameliorated this issue by replacing all 
date values for titles that were supposedly released in 1970 with nan values, as they all seemed to be false. However, I did not do this for 12/31/2020 and 12/31/2021 which also
seemed to be false release dates. I made this sacrifice for the sake of longevity. If you are using this code in 2021, there very well may have been titles released on 12/31/2020,
Generally though, if you are using this data set and plan to incorporate release date in your analysis, be wary of these false dates which tend to fall on the extremes of the
future calendar and 1970.
