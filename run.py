from crawler import Crawler
from data_cleaner import DataCleaner

# Get the top500 players of all servers

servers_list = ['br', 'na', 'latam', 'eu', 'kr', 'ap']
top500_all_servers = Crawler.get_top500_all_servers(servers_list)

# Get all top500 player's matches

_players_list = top500_all_servers['leaderboards.full_nickname']
players_list = _players_list.to_list()

top500_players_matches_report = Crawler.get_top500_players_matches_report(players_list)