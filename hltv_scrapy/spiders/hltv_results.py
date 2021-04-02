import logging
import re

import pymongo
import scrapy
from hltv_scrapy.items import HLTVResultItem

logging.basicConfig(
    filename='hltv_results.log',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)


class HLTVResultsSpider(scrapy.Spider):
    name = "hltv_results"

    allowed_domains = ['hltv.org']
    start_urls = ['https://www.hltv.org/results']

    matches_col = 'matches'  # TODO: Use these
    # skipped_col ...

    def __init__(self, mongo_uri, mongo_db):
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[mongo_db]

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def parse(self, response):
        '''
        Iterate through pages of results and parse each match

        '''
        results = response.css('.results-holder')[-1].css('.result-con')
        self.num_results = self._get_num_results(response)

        # Efficient algorithm
        latest_db_match = self.db['matches'].find_one(
            {}, sort=[('end_time', -1)])
        latest_hltv_match_endtime = int(results[0].css(
            'div::attr(data-zonedgrouping-entry-unix)').get())
        # Get new results if there is any
        if latest_db_match['end_time'] < latest_hltv_match_endtime:
            yield from self._scrape_all(response, url_stop=latest_db_match['url'])
        else:
            # Efficient search for missing matches
            num_db_results = self.db['matches'].count_documents()
            yield from self._recursive_search()

        # existing_matches = 0
        # skipped_matches = 0
        # for result in results: # Temp limit
        #     url = result.css('a::attr(href)').get()
        #     end_time = result.css('div::attr(data-zonedgrouping-entry-unix)').get()
        #     # Check if match already exists in database
        #     if self.db['matches'].find({'url': url}).count():
        #         existing_matches += 1
        #         # logger.debug(f'Match already exists in database. skipping match <{url}>')
        #         continue
        #     if self.db['skipped_matches'].find({'url': url}).count():
        #         skipped_matches += 1
        #         continue

        #     yield scrapy.Request(response.urljoin(url), callback=self.parse_match,
        #                          cb_kwargs=dict(url=url, end_time=end_time))
        # # Log number of matches in db per page
        # if existing_matches:
        #     logger.debug(f'{existing_matches} (+{skipped_matches})/{len(results)} already exist in database on this page')

        # # Go to next page
        # next_page_url = response.css('.pagination-next::attr(href)')[0]
        # next_page_offset = int(next_page_url.re(r'=([0-9]+)')[0]) # Results offset
        # if next_page_offset < 0: # Temp page limit
        #     yield response.follow(next_page_url, callback=self.parse)

    def parse_match(self, response, url, end_time):
        '''
        Scrape match page for stats and return match item

        '''
        matchItem = HLTVResultItem()
        matchItem['url'] = url
        matchItem['end_time'] = end_time

        _page = response.css('.match-page')

        matchItem['start_time'] = _page.css('.time::attr(data-unix)').get()
        matchItem['event_url'] = _page.css('.event').css('a::attr(href)').get()

        _team1_box = _page.css('.team1-gradient')
        _team2_box = _page.css('.team2-gradient')
        matchItem['team1_url'] = _team1_box.css('a::attr(href)').get()
        matchItem['team2_url'] = _team2_box.css('a::attr(href)').get()
        matchItem['team1_score'] = _team1_box.css(
            '.won, .lost, .draw, .tie').css('::text').get()
        matchItem['team2_score'] = _team2_box.css(
            '.won, .lost, .draw, .tie').css('::text').get()

        # Get match description and map vetos
        _maps_section = _page.css('.maps')
        _info_boxes = _maps_section.css('.maps .veto-box')

        matchItem['info_boxes'] = []
        for info_box in _info_boxes:
            lines = info_box.css('::text').getall()
            info = ''.join(line.strip(' ') for line in lines)
            matchItem['info_boxes'].append(info)

        # try:
        #     # matchItem['description'] = _info_boxes[0].css(' *::text').getall()
        #     matchItem['description'] = _info_boxes[0].css('::text').getall()
        #     matchItem['veto'] = _info_boxes[1].css('::text').getall()
        # except IndexError:
        #     pass

        # TODO: Do this after player stats and use for validation
        # Get results for each map
        matchItem['match_results'] = []
        for _map_result in _maps_section.css('.mapholder'):
            map_dict = {}
            map_dict['mapname'] = _map_result.css('.mapname::text').get()
            map_dict['team1_score'] = _map_result.css(
                '.results-left .results-team-score::text').get()
            map_dict['team2_score'] = _map_result.css(
                '.results-right .results-team-score::text').get()

            matchItem['match_results'].append(map_dict)

        # Get player stats
        _match_stats = _page.css('.matchstats')
        tab_names = _match_stats.css('.dynamic-map-name-full::text').getall()
        matchItem['match_stats'] = []
        for i, _map_stats in enumerate(_match_stats.css('.stats-content')):
            # if i == 0: # Ignore first 'all maps' stats table (also present in best of 1 matches)
            #     continue
            map_dict = {}
            map_dict['mapname'] = tab_names[i]
            map_dict['team1'] = {}
            map_dict['team2'] = {}

            # Go through both teams
            for x in range(2):
                team = map_dict['team1']
                if x == 1:
                    team = map_dict['team2']

                team['stats'] = []

                # Loop through each players stats
                for j, _player_stats in enumerate(_map_stats.css('.totalstats')[x].css('tr')):
                    if j == 0:  # Get the team url from the first row of stats table
                        team['team_url'] = _player_stats.css(
                            'a::attr(href)').get()
                        continue

                    try:
                        player_stats = {}
                        player_stats['player_url'] = _player_stats.css(
                            'a::attr(href)').get()
                        player_stats['username'] = _player_stats.css(
                            '.player-nick::text').get()
                        player_stats['kills'] = _player_stats.css(
                            '.kd::text').re('[0-9]+')[0]
                        player_stats['deaths'] = _player_stats.css(
                            '.kd::text').re('[0-9]+')[1]
                        player_stats['adr'] = _player_stats.css(
                            '.adr::text').re('[0-9]+.[0-9]+')[0]
                        player_stats['kast'] = _player_stats.css(
                            '.kast::text').re('[0-9]+.[0-9]+')[0]
                        player_stats['rating'] = _player_stats.css(
                            '.rating::text').re('[0-9]+.[0-9]+')[0]
                        team['stats'].append(player_stats)
                    except IndexError:
                        matchItem['skip_match'] = True
                        break

            matchItem['match_stats'].append(map_dict)

            # for i in range(2):
            #     team_url = matchItem['team2_url']
            #     if i == 0:
            #         team_url = matchItem['team1_url']

            #     if self.db['teams'].find({'url': team_url}).count():
            #         self.debug(f'Team already in database.  <{team_url}>')
            #         continue

            #     self.scrape_team(team_url)

        yield matchItem

    def _scrape_all(self, response, url_stop=None, unix_time_stop=0, callback=None):
        '''
        Scrape all match results starting from the most recent

        response: scrapy.Response Object
        url_stop: str - Match url to stop scrapng at
        unix_time_stop: int -  Unix time to stop scraping at

        '''
        results = response.css('.results-holder')[-1].css('.result-con')

        for result in results:
            url = result.css('a::attr(href)').get()
            end_time = int(result.css(
                'div::attr(data-zonedgrouping-entry-unix)').get())
            # Finish scraping when it hits the url_stop or unix_time_stop
            if url == url_stop or end_time <= unix_time_stop:
                if callback:
                    callback()
                return
            # Skip matches already in db
            if self.db['matches'].find({'url': url}).count():
                continue

            yield scrapy.Request(response.urljoin(url), callback=self.parse_match,
                                 cb_kwargs=dict(url=url, end_time=end_time))
        # Go to next page
        next_page_url = response.css('.pagination-next::attr(href)')[0]
        yield response.follow(next_page_url, callback=self._scrape_all)

    def _recursive_search(self, num_results=None):
        if num_results == None:
            num_results = self.db['matches']

        last_db_endtime = int(self.db['matches'].find().sort(
            'end_time', 1)[0]['end_time'])
        print(last_db_endtime)

    def _get_num_results(self, response):
        ''' Returns an integer number of results '''
        return int(response.css('.pagination-data::text')[0].re(r'of ([0-9]+)')[0])

    def _is_match_on_page(self, response, end_time):
        '''      

        '''
        return

    def check_matches(self):
        # TODO: query.explain('executionStats') optimization
        # end_times = self.db['matches'].find().sort('end_time', -1)
        # for doc in end_times:
        #     print(doc['end_time'])
        pass

### TESTS/EDGE CASES ###
# bo1 forfeit https://www.hltv.org/matches/2344055/imperium-vs-swole-patrol-esea-mdl-season-35-north-america
# 6-man roster for Vitality https://www.hltv.org/matches/2344829/vitality-vs-big-blast-premier-fall-series-2020
# Royal Republic changed org https://www.hltv.org/matches/2345493/havan-liberty-vs-royal-republic-liga-dell-11-nov-20
# Map advantage from upper bracket https://www.hltv.org/matches/2345311/teamone-vs-new-england-whalers-esea-mdl-season-35-north-america
# Pre-determined subs, "...will play instead of..." https://www.hltv.org/matches/2345732/natus-vincere-vs-astralis-blast-premier-fall-2020-finals
# Sub https://www.hltv.org/matches/2345989/project-x-vs-lilmix-vulkan-fight-series-2020
