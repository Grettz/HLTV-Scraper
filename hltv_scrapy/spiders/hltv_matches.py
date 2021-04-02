import scrapy

class HLTVMatchesSpider(scrapy.Spider):
    name = "hltv_matches"
    
    start_urls = [
        'http://www.hltv.org/matches'
    ]
    
    def parse(self, response):
        pass
    