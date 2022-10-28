from aws_s3 import AwsS3
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
from pandas import read_csv
import requests
import json
from data_cleaner import DataCleaner

class Crawler():

    def get_top500_all_servers(servers_list) -> str:
        '''
            This function's mission is to get all players that are ranked in the top 500 of a server list.
            :param [list] server_list: A variable that receives a server list. For example: 'kr', 'eu', 'na', 'br', 'latam', 'ap'.
            :return [pd.dataframe] data: A variable that receives a string with the structure of a json. This string contains the summarized information of a player that who's in top 500.
        '''

        path_write = 'raw/trackergg/rank/top500/all_servers/'
        data = []
        df = pd.DataFrame()
        file_format = '.csv'

        for server in servers_list:
            
            for page in range(1, 5):

                response = requests.get('https://val.dakgg.io/api/v1/leaderboards/{}/aca29595-40e4-01f5-3f35-b1b3d304c96e?page={}&tier=top500'.format(server, page))
                data_aux = response.json()
                data.append(data_aux)

            data_upload = str(data)
            AwsS3.upload_file(data_upload, path_write, file_format)


        df = pd.DataFrame(data)
        df = pd.json_normalize(json.loads(df.to_json(orient='records'))).explode('leaderboards')
        df = pd.json_normalize(json.loads(df.to_json(orient='records')))

        df['leaderboards.full_nickname'] = (df['leaderboards.gameName'].map(str) + '%23' + df['leaderboards.tagLine'].map(str))
        df.to_csv('data/top500_all_servers.csv')
            
        return df

    
    def get_player_matches_report(player_name_tag) -> str:
        '''
            This function's mission is to get a summary report of all the last 200 matches of a specific player.
            :param [str] player_name_tag: A variable that receives a player's nickname. For example: RayzenSama%236999 .
            :return [str] data_pre: A variable that receives a string with the structure of a json. This string contains the summarized information of a player.
        '''

        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Remote('http://127.0.0.1:4444/wd/hub', options = options)

        path_write = 'raw/trackergg/matches_report/player'
        file_format = '.txt'

        for page in range(0,10):
            
            driver.get('https://api.tracker.gg/api/v2/valorant/standard/matches/riot/{}?type=competitive&next={}'.format(player_name_tag, page))
            data_pre = driver.find_element('xpath', '//pre').text

            AwsS3.upload_file(data_pre, path_write, file_format)
            time.sleep(2)

            driver.quit()

        return data_pre


    def get_top500_players_matches_report(players_list) -> str:
        '''
            This function's mission is to get a summary report of all the last 200 matches of a specific player.
            :param [list] players_list: A variable that receives a players list. For example: ['NaraKa%232299','NakaRa%233265','RayzenSama%236999'].
            :retunr [str] data_pre: A variable that receives a string with the structure of a json. This string contains the summarized information of a player who's in the top 500.
        '''

        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Remote('http://127.0.0.1:4444/wd/hub', options = options)

        players_list = DataCleaner._remove_duplicates(players_list)
        path_write = 'raw/trackergg/matches_report/top500/all_servers'
        file_format = '.txt'
        data = []
        for player in players_list:

            for page in range(0,10):
                
                start = time.time()
                driver.get('https://api.tracker.gg/api/v2/valorant/standard/matches/riot/{}?type=competitive&next={}'.format(player, page))
                data_pre = driver.find_element('xpath', '//pre').text
                data.append(data_pre)
                end = time.time()
                print('{} - {}'.format(player, page))
                total_time = end - start
                print("\n"+ str(total_time))

                AwsS3.upload_file(data_pre, path_write, file_format)

        driver.quit()


        return data


    def get_matches_report_detail(matches_list) -> str:
        ''''
            This function's mission is to get a detail report of a match.
            :param [list] matches_list: A variable that receives a matches id list. For example: 2bee0dc9-4ffe-519b-1cbd-7fbe763a6047.
            :return [str] data_pre: A variable that receives a string with the structure of a json. This string contains the detailed information of matches.
        '''

        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Remote('http://127.0.0.1:4444/wd/hub', options = options)

        path_write = 'raw/trackergg/matches_report_details/top500/all_servers'
        file_format = '.txt'

        # matches = pd.read_csv("matches.csv")
        # matches = matches['match_id'].to_list()

        for matche in matches_list:
            
            driver.get('https://api.tracker.gg/api/v2/valorant/standard/matches/{}'.format(matche))
            data_pre = driver.find_element('xpath', '//pre').text
            time.sleep(5)
            
            AwsS3.upload_file(data_pre, path_write, file_format)

        driver.quit()

        return data_pre


    def get_gun_report() -> str:

        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

        path_write = 'raw/trackergg/gun_report/'
 
        driver.get('https://api.tracker.gg/api/v2/valorant/standard/profile/riot/RayzenSama%236999/segments/weapon?playlist=competitive&seasonId=')
        
        data_pre = driver.find_element('xpath', '//pre').text
        file_format = '.txt'
        
        AwsS3.upload_file(data_pre, path_write, file_format)

        driver.quit()