import pandas as pd
from selenium import webdriver
import time
import json
import statistics

df = pd.read_csv('data/top500_all_servers.csv')

def get_top500_players_summarized_matches_report(players_list) -> str:
    '''
        This function's mission is to get a summary report of all the last 200 matches of a specific player.
        :param [list] players_list: A variable that receives a players list. For example: ['NaraKa%232299','NakaRa%233265','RayzenSama%236999'].
        :return [str] data_pre: A variable that receives a string with the structure of a json. This string contains the summarized information of a player who's in the top 500.
    '''
    driver = webdriver.Chrome()
    data = []
    player_time=[]
    page_time=[]
    for player in players_list:

        start_player = time.time()
        

        for page in range(1,10):

            start_page = time.time()

            
            driver.get('https://api.tracker.gg/api/v2/valorant/standard/matches/riot/{}?type=competitive&next={}'.format(player, page))
            data_pre = driver.find_element('xpath', '//pre').text
            data_json = json.loads(data_pre)
            if 'data' in data_json.keys():
                matches = data_json['data']['matches']
                for match in matches:
                    dataframe_row = dict()
                    dataframe_row['player'] = player
                    dataframe_row['matchId'] = match['attributes']['id']
                    dataframe_row['mapId'] = match['attributes']['mapId']
                    dataframe_row['modeId'] = match['attributes']['modeId']
                    dataframe_row['modeKey'] = match['metadata']['modeKey']
                    dataframe_row['modeName'] = match['metadata']['modeName']
                    dataframe_row['modeImageUrl'] = match['metadata']['modeImageUrl']
                    dataframe_row['modeMaxRounds'] = match['metadata']['modeMaxRounds']
                    dataframe_row['isAvailable'] = match['metadata']['isAvailable']
                    dataframe_row['timestamp'] = match['metadata']['timestamp']
                    dataframe_row['metadataResult'] = match['metadata']['result']
                    dataframe_row['map'] = match['metadata']['map']
                    dataframe_row['mapName'] = match['metadata']['mapName']
                    dataframe_row['mapImageUrl'] = match['metadata']['mapImageUrl']
                    dataframe_row['seasonName'] = match['metadata']['seasonName']
                    dataframe_row['userId'] = match['segments'][0]['attributes']['platformUserIdentifier']
                    dataframe_row['hasWon'] = match['segments'][0]['metadata']['hasWon']
                    dataframe_row['result'] = match['segments'][0]['metadata']['result']
                    dataframe_row['agentName'] = match['segments'][0]['metadata']['agentName']
                    for stat_name, stat_value in match['segments'][0]['stats'].items():
                        dataframe_row[f"{stat_name}Value"] = stat_value['value']
                        dataframe_row[f"{stat_name}DisplayValue"] = stat_value['displayValue']
                        dataframe_row[f"{stat_name}DisplayType"] = stat_value['displayType']
                    data.append(dataframe_row)
            end_page = time.time()
            total_time_page = (end_page - start_page)*10
            page_time.append(total_time_page)
    
        end_player = time.time()
        total_time_player = end_player - start_player
        player_time.append(total_time_player)

    df = pd.DataFrame.from_dict(data)
    df.to_csv('data/matches_summarized.csv',index=False)

    driver.quit()
    print(f"Average time to process a page: {statistics.mean(page_time)}")
    print(f"Average time to process a player: {statistics.mean(player_time)}")

    return df

def get_top500_players_weapons_report(players_list) -> str:
    '''
        This function's mission is to get a summary report of all the last 200 matches of a specific player.
        :param [list] players_list: A variable that receives a players list. For example: ['NaraKa%232299','NakaRa%233265','RayzenSama%236999'].
        :return [str] data_pre: A variable that receives a string with the structure of a json. This string contains the summarized information of a player who's in the top 500.
    '''
    driver = webdriver.Chrome()
    data = []
    player_time=[]
    page_time=[]
    for player in players_list:

        start_player = time.time()
        
        start_page = time.time()
        
        driver.get('https://api.tracker.gg/api/v2/valorant/standard/profile/riot/{}/segments/weapon?playlist=competitive&seasonId=default'.format(player))
        data_pre = driver.find_element('xpath', '//pre').text
        data_json = json.loads(data_pre)
        if 'data' in data_json.keys():
            weapons = data_json['data']
            for weapon in weapons:
                dataframe_row = dict()
                dataframe_row['player'] = player
                dataframe_row['weaponName'] = weapon['attributes']['key']
                for stat_name, stat_value in weapon['stats'].items():
                    dataframe_row[f"{stat_name}Value"] = stat_value['value']
                    dataframe_row[f"{stat_name}DisplayValue"] = stat_value['displayValue']
                    dataframe_row[f"{stat_name}DisplayType"] = stat_value['displayType']
                data.append(dataframe_row)
        end_page = time.time()
        total_time_page = (end_page - start_page)*10
        page_time.append(total_time_page)
    
        end_player = time.time()
        total_time_player = end_player - start_player
        player_time.append(total_time_player)

    df = pd.DataFrame.from_dict(data)
    df.to_csv('data/weapons_summarized.csv',index=False)
    driver.quit()
    print(f"Average time to process a page: {statistics.mean(page_time)}")
    print(f"Average time to process a player: {statistics.mean(player_time)}")

    return df

get_top500_players_summarized_matches_report(df['leaderboards.full_nickname'])
print("DONE!")
# get_top500_players_weapons_report(df['leaderboards.full_nickname'])

# print(df['leaderboards.full_nickname'][0])