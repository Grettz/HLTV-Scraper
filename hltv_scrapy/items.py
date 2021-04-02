# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class HLTVResultItem(Item):
    url = Field()
    skip_match = Field()
    start_time = Field()
    end_time = Field()
    event_url = Field()
    team1_url = Field()
    team2_url = Field()
    team1_score = Field()
    team2_score = Field()
    best_of = Field()
    substitutes = Field()
    info_boxes = Field()
    # description = Field()
    # veto = Field()
    match_results = Field()
    match_stats = Field()
    
    def __repr__(self):
        return repr(f'{self["team1_url"].split("/")[-1]} {self["team1_score"]} - {self["team2_score"]} {self["team2_url"].split("/")[-1]}')
    
class HLTVTeamItem(Item):
    url = Field()
    teamname = Field()
    logo_url = Field()
    
class HLTVPlayerItem(Item):
    url = Field()
    username = Field()
    first_name = Field()
    last_name = Field()
    # logo = Field()
    # country = Field()