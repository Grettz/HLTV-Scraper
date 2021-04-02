from scrapy.crawler import CrawlerProcess
from hltv_scrapy.spiders.hltv_results import HLTVResultsSpider
from scrapy.utils.project import get_project_settings

if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl(HLTVResultsSpider)
    # process.crawl(HLTVTeamsSpider)
    # process.crawl(HLTVPlayersSpider)
    process.start() # the script will block here until the crawling is finished