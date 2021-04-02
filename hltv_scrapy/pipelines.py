# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import math
from collections import defaultdict

import pymongo
from itemadapter import ItemAdapter
from scrapy import Request
from scrapy.exceptions import DropItem

from hltv_scrapy.items import HLTVTeamItem

# TODO: Validate matches, find missing or deleted matches, etc. against hltv database
logging.basicConfig(
    filename='application.log',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)


class HLTVResultsPipeline:
    '''
    Validate match data and store match in MongoDB
    '''
    matches_col = 'matches'
    teams_col = 'teams'
    players_col = 'players'

    scraped = defaultdict(int)

    def __init__(self, crawler):
        self.crawler = crawler  # Not currently used for anything
        self.mongo_uri = crawler.settings.get('MONGO_URI')
        self.mongo_db = crawler.settings.get('MONGO_DATABASE')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler=crawler)

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        logger.info(f'{self.scraped["matches"]} matches scraped. '
                    f'{self.scraped["skipped_matches"]} matches skipped.')
        self.client.close()

    def process_item(self, item, spider):
        if item.get('skip_match'):
            self.skip_match(item, f'Match skipped from spider ({spider.name})')

        # Validate match data
        try:
            item['start_time'] = int(item['start_time'])
            item['end_time'] = int(item['end_time'])
            item['team1_score'] = int(item['team1_score'])
            item['team2_score'] = int(item['team2_score'])
        except (TypeError, ValueError):
            self.skip_match(item, f'Valuable data is missing from match')

        # Verify best of
        best_of = len(item['match_results'])
        # bo1=1, bo2=1, bo3=2, bo5=3 etc.
        score_to_win_or_draw = math.ceil(best_of / 2)
        winning_score = max(item['team1_score'], item['team2_score'])
        if best_of > 1 and winning_score != score_to_win_or_draw:
            self.skip_match(
                item, f'Match score ({item["team1_score"]} - {item["team2_score"]}) does not reflect "best of" length ({best_of})')
        item['best_of'] = best_of

        # Verify map scores
        for result in item['match_results']:
            try:
                result['team1_score'] = int(result['team1_score'])
                result['team2_score'] = int(result['team2_score'])
            except (TypeError, ValueError):
                if result['team1_score'] != '-':
                    self.skip_match(item, f'Map score(s) unavailable in match')

        self.find_substitutes(item)

        # Verify match stats
        if not item['match_stats']:
            self.skip_match(item, f'Stats table is missing from match')

        # Verify number of players. Skip non-5 player team stats for now
        # TODO: implement subs/stand-ins into model and not skip match
        overall_stats = item['match_stats'][0]
        for team_stats in [overall_stats['team1']['stats'],
                           overall_stats['team2']['stats']]:
            if (num_players := len(team_stats)) < 5:
                self.skip_match(
                    item, f'Match contains less than 5 players on a team ({num_players})')
            # Let 6-man rosters/stand-ins through, will be dealt with in model logic
            elif num_players > 5:
                item['substitutes'] = True

        # Check if match exists in db
        if self.db[self.matches_col].find({'url': item['url']}).count():
            raise DropItem(f'Match already exists in database <{str(item)}>')

        # Add match to db
        self.db[self.matches_col].insert_one(ItemAdapter(item).asdict())
        logger.debug(f'Added match to database <{str(item)}>')
        self.scraped['matches'] += 1

        # Add match to teams match history
        for team_url in [item['team1_url'], item['team2_url']]:
            # Add team to db if it doesnt exist
            if not self.db[self.teams_col].find_one({'url': team_url}):
                self.db[self.teams_col].insert_one({
                    'url': team_url,
                })
                logger.debug(f'Added team to database <{team_url}>')

            # Append match to teams match history
            self.db[self.teams_col].update_one(
                {'url': team_url},
                {'$addToSet': {'match_history': {
                    'url': item['url'],
                    'end_time': item['end_time']
                }}}
            )

        # Add match to players match history
        for team_stats in [overall_stats['team1']['stats'],
                           overall_stats['team2']['stats']]:
            for player_stats in team_stats:
                # Add player to db if it doesnt exist
                player_url = player_stats['player_url']
                if not self.db[self.players_col].find_one({'url': player_url}):
                    self.db[self.players_col].insert_one({
                        'url': player_url,
                    })
                    logger.debug(f'Added player to database <{player_url}>')

                # Append match to players match history
                self.db[self.players_col].update_one(
                    {'url': player_url},
                    {'$addToSet': {'match_history': {
                        'url': item['url'],
                        'end_time': item['end_time']
                    }}}
                )

        # if bo > 1:
        #     subbed_players = [sub := player for player in player_stats if player not in list()]
        # Call self.substitute_handler method?
        # player_urls = map()?

        return item

    def find_substitutes(self, item):
        ''' Currently just returns number of subs '''
        # Description search
        key_phrases = [
            'substitute',
            'subbed',
            'stand in',
            'stand-in',
            'will play for'
        ]
        subs = 0
        for phrase in key_phrases:  # TODO: RE-DO
            if not item['info_boxes']:
                break
            if phrase not in item['info_boxes'][0].lower():
                continue
            subs += 1
            # TODO
        # if subs:
        logger.debug(f'{subs} substitutes found in match <{str(item)}>')
        return subs

    def skip_match(self, item, reason):
        ''' Put the match into the skipped_matches collection and drop it '''
        self.db['skipped_matches'].update_one({'url': item['url']}, {
            '$set': {'end_time': item['end_time']},
            '$addToSet': {'reason': reason}
        }, upsert=True)
        self.scraped['skipped_matches'] += 1

        raise DropItem(reason, f'<{item["url"]}>')
