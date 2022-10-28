import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
from botocore.exceptions import ClientError
import logging

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")

class AwsS3():

    
    def upload_file(data : object, path : str, file_format) -> bool:

        """
            Upload a file to an S3 bucket
            :param file_name: File to upload
            :param bucket: Bucket to upload to
            :param object_name: S3 object name. If not specified then file_name is used
            :return: True if file was uploaded, else False
        """

        date = datetime.now().strftime("_%Y%m%d_%H%M%S")
        file_name = 'valorant_reports{}{}'.format(date, file_format)
        input = path + file_name

        
        s3 = boto3.client("s3", aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

        try:
            s3.put_object(Bucket = AWS_S3_BUCKET, Body = data, Key = input)

        except ClientError as e:
            logging.error(e)

            return False

        return True

    
    def get_file(path : str, file_name : str) -> str:

        """
            Get a file to an S3 bucket
            :param Path: Path to get
            :param bucket: Bucket to upload to
            :param object_name: S3 object name. If not specified then file_name is used
            :return: True if file was uploaded, else False
        """
        s3 = boto3.client('s3')
        
        try:

            response = s3.get_object(Bucket = AWS_S3_BUCKET, Key = file_name)
            data = response['Body'].read()
            data_str = data.decode('utf-8')

        except ClientError as e:
            logging.error(e)


        return data_str
        

    def get_files_list(path_read : str) -> list:

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(AWS_S3_BUCKET)
        files_list = bucket.objects.filter(Prefix = path_read)
        files_list = list(files_list)
        
        if len(files_list) > 1: 
            del files_list[0]
        else:
            pass

        return files_list

from aws_s3 import AwsS3
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
import requests
import json
from datetime import datetime

from pandas import read_csv



class Crawler():




    def get_top500_all_servers(servers_list) -> str:
        '''
            This function's mission is to get all players that are ranked in the top 500 of a server list.

            Input:
                [list] server_list: A variable that receives a server list. For example: 'kr', 'eu', 'na', 'br', 'latam', 'ap'.
            
            Output:
                [str] data: A variable that receives a string with the structure of a json. This string contains the summarized information of a player that who's in top 500.
        '''
        path_write = 'raw/trackergg/rank/top/all_servers'
        data = []
        df = pd.DataFrame()
        file_format = '.csv'

        for server in servers_list:
            
            for page in range(1, 5):
                response = requests.get('https://val.dakgg.io/api/v1/leaderboards/{}/aca29595-40e4-01f5-3f35-b1b3d304c96e?page={}&tier=top500'.format(server, page))
                data_aux = response.json()
                data.append(data_aux)


        df = pd.DataFrame(data)
        df = pd.json_normalize(json.loads(df.to_json(orient='records'))).explode('leaderboards')
        df = pd.json_normalize(json.loads(df.to_json(orient='records')))

        df['leaderboards.full_nickname'] = (df['leaderboards.gameName'].map(str) + '%23' + df['leaderboards.tagLine'].map(str))

        df.to_csv('data/top500_all_servers.csv')

        data = read_csv('data/top500_all_servers.csv')



        #TODO: Arrumar o upload para o S3. ERRO: expected string or bytes-like object
        # AwsS3.upload_file(data, path_write, file_format)

            
        # return data

    
    def get_player_matches_report(player_name_tag) -> str:
        '''
            This function's mission is to get a summary report of all the last 200 matches of a specific player.

            Input:
                [str] player_name_tag: A variable that receives a player's nickname. For example: RayzenSama%236999 .
            Output:
                [str] data_pre: A variable that receives a string with the structure of a json. This string contains the summarized information of a player.
        '''

        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Remote('http://127.0.0.1:4444/wd/hub', options = options)

        path_write = 'raw/trackergg/matches_report/player'
        file_format = '.txt'
        

        for page in range(0,10):
            
            driver.get('https://api.tracker.gg/api/v2/valorant/standard/matches/riot/{}?type=competitive&next={}'.format(player_name_tag, page))
            data_pre = driver.find_element('xpath', '//pre').text
            time.sleep(5)

            AwsS3.upload_file(data_pre, path_write, file_format)

            driver.quit()


        return data_pre



    def get_top500_players_matches_report(players_list) -> str:
        '''
            This function's mission is to get a summary report of all the last 200 matches of a specific player.

            Input:
                [list] players_list: A variable that receives a players list. For example: ['NaraKa%232299','NakaRa%233265','RayzenSama%236999'] .
            Output:
                [str] data_pre: A variable that receives a string with the structure of a json. This string contains the summarized information of a player who's in the top 500.
        '''

        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Remote('http://127.0.0.1:4444/wd/hub', options = options)

        path_write = 'raw/trackergg/matches_report/top500/all_servers/'
        file_format = '.txt'


        for player in players_list:

            data = []
            startTime_player = datetime.now()


            for page in range(0,10):
                
                startTime_page = datetime.now()
                
                driver.get('https://api.tracker.gg/api/v2/valorant/standard/matches/riot/{}?type=competitive&next={}'.format(player, page))
                data_pre = driver.find_element('xpath', '//pre').text
                data.append(data_pre)
                print(datetime.now() - startTime_player)
                print('{} - {}'.format(player, page))
                print(datetime.now() - startTime_page)

            print(datetime.now() - startTime_player)

            data_upload = str(data)
            AwsS3.upload_file(data_upload, path_write, file_format)


        driver.quit()


        # return data


    def get_matches_report_detail(matches_list) -> str:
        ''''
            This function's mission is to get a detail report of a match.

            Input:
                [list] matches_list: A variable that receives a matches id list. For example: 2bee0dc9-4ffe-519b-1cbd-7fbe763a6047.
            Output:
                [str] data_pre: A variable that receives a string with the structure of a json. This string contains the detailed information of matches.
        '''

        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Remote('http://127.0.0.1:4444/wd/hub', options = options)

        path_write = 'raw/trackergg/matches_report_details/top500/'
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


        return data_pre

from aws_s3 import AwsS3
import pandas as pd
import json

class DataCleaner():
    
    def data_cleaner_matches():

        path_read = 'raw/trackergg/matches_report/top500/'
        path_write = 'cleaned/trackergg/matches_report/top500/all_servers/'
        
        df_aux = pd.DataFrame()

        files = AwsS3.get_files_list(path_read)

        for file in files:
            file = file.key
            data_s3 = AwsS3.get_file(path_read, file)
            data_json = json.loads(data_s3)
            
            expiryDate : str = data_json["data"]["expiryDate"]
            requestingPlayerAttributes : dict = data_json["data"]["requestingPlayerAttributes"]
            paginationType : str = data_json["data"]["paginationType"]
            metadata : dict = data_json["data"]["metadata"]
            matches : list = data_json["data"]["matches"]

            data = []

            for match in matches:
                attributes : dict = match["attributes"]
                match_metadata : dict = match["metadata"]
                expiryDate : str = match["expiryDate"]
                
                segments : list = match["segments"]
                for segment in segments:
                    segment_type: str = segment["type"]
                    attributes : dict = segment["attributes"]
                    segment_metadata : dict = segment["metadata"]
                    expiryDate : str = segment["expiryDate"]
                    
                    stat_dict = {}
                    stats : dict = segment["stats"]
                    for stat, stat_data in stats.items():
                        stat_keys = stat_data.keys()
                        stat_columns = [f'{stat}_{col}' for col in stat_keys]
                        stat_values = stat_data.values()
                        _stat_dict = {k: v for k, v in zip(stat_columns, stat_values)}
                        stat_dict.update(_stat_dict)

                    row = {}
                    row.update(attributes)
                    row.update(match_metadata)
                    row["expiryDate"] = expiryDate
                    row["segment_type"] = segment_type
                    row.update(attributes)
                    row.update(segment_metadata)
                    row["expiryDate"] = expiryDate
                    row["match_id"] = match["attributes"]["id"]
                    row.update(stat_dict)

                    data.append(row)

            columns = data[0].keys()
            df = pd.DataFrame(data, columns=columns)
            df_aux = pd.concat([df_aux, df], axis = 0)

        df_final = pd.concat([df_aux, df_aux['rank_metadata'].apply(pd.Series)], axis=1)

        df_final.to_csv('data/matches.csv')

        data_final_csv = df_final.to_csv()

        file_format = '.csv'

        AwsS3.upload_file(data_final_csv, path_write, file_format)

        return df_final

   
    def data_cleaner_matches_details():
        
        path_read = 'raw/trackergg/matches_report_details/top500/all_servers'
        path_write = 'cleaned/trackergg/matches_report_details/top500/all_servers'     

        files = AwsS3.get_files_list(path_read)

        metadata_dict_list = []
        player_round_dict_list = []
        player_round_damage_dict_list = []
        player_summary_dict_list = []
        player_round_kills = []

        for file in files:
            data_json = json.loads(AwsS3.get_file(path_read, file.key))
            metadata : dict = data_json["data"]["metadata"]
            metadata['match_id'] = data_json['data']["attributes"]["id"]
            metadata_dict_list.append(metadata)

            segments = data_json['data']['segments']

            for segment in segments:
                if segment['type'] == 'player-round':
                    segment_dict = {}
                    attributes = segment['attributes']
                    segment_dict.update(attributes)
                    metadata = segment['metadata']
                    segment_dict.update(metadata)
                    segment_stats = segment['stats']
                    segment_stats_dict = {}
                    for stat, stat_data in segment_stats.items():
                        stat_keys = stat_data.keys()
                        stat_columns = [f'{stat}_{col}' for col in stat_keys]
                        stat_values = stat_data.values()
                        _stat_dict = {k: v for k, v in zip(stat_columns, stat_values)}
                        segment_stats_dict.update(_stat_dict)
                    segment_dict.update(segment_stats_dict)
                    segment_dict['match_id'] = data_json['data']["attributes"]["id"]
                    player_round_dict_list.append(segment_dict)

                elif segment['type'] == 'player-round-damage':
                    segment_dict = {}
                    attributes = segment['attributes']
                    segment_dict.update(attributes)
                    segment_stats = segment['stats']
                    segment_stats_dict = {}
                    for stat, stat_data in segment_stats.items():
                        stat_keys = stat_data.keys()
                        stat_columns = [f'{stat}_{col}' for col in stat_keys]
                        stat_values = stat_data.values()
                        _stat_dict = {k: v for k, v in zip(stat_columns, stat_values)}
                        segment_stats_dict.update(_stat_dict)
                    segment_dict.update(segment_stats_dict)
                    segment_dict['match_id'] = data_json['data']["attributes"]["id"]
                    player_round_damage_dict_list.append(segment_dict)
                
                elif segment['type'] == 'player-summary':
                    segment_dict = {}
                    attributes = segment['attributes']
                    segment_dict.update(attributes)
                    metadata = segment['metadata']
                    segment_dict.update(metadata)
                    segment_stats = segment['stats']
                    segment_stats_dict = {}
                    for stat, stat_data in segment_stats.items():
                        try:
                            stat_keys = stat_data.keys()
                            stat_columns = [f'{stat}_{col}' for col in stat_keys]
                            stat_values = stat_data.values()
                            _stat_dict = {k: v for k, v in zip(stat_columns, stat_values)}
                            segment_stats_dict.update(_stat_dict)
                        except AttributeError:
                            """Tratativa de excecao para o caso em que nao tivermos alguma coluna dentro dos status"""
                            pass
                    segment_dict.update(segment_stats_dict)
                    segment_dict['match_id'] = data_json['data']["attributes"]["id"]
                    player_summary_dict_list.append(segment_dict)

                elif segment['type'] == 'player-round-kills':
                    segment_dict = {}
                    attributes = segment['attributes']
                    segment_dict.update(attributes)
                    metadata = segment['metadata']
                    segment_metadata_dict = {}
                    for metadata_iter, metadata_data in metadata.items():
                        try:
                            metadata_keys = metadata_data.keys()
                            metadata_columns = [f'{metadata_iter}_{col}' for col in metadata_keys]
                            metadata_values = metadata_data.values()
                            _metadata_dict = {k: v for k, v in zip(metadata_columns, metadata_values)}
                            segment_metadata_dict.update(_metadata_dict)
                        except AttributeError:
                            """Tratativa de excecao para o caso em que nao tivermos alguma coluna dentro dos status"""
                            metadata_weaponImageUrl = metadata['weaponImageUrl']
                            metadata_weaponName = metadata['weaponName']
                            metadata_weaponCategory = metadata['weaponCategory']
                            metadata_gameTime = metadata['gameTime']
                            metadata_roundTime = metadata['roundTime']
                            pass
                        segment_stats_damage = segment['stats']['damage']
                        segment_metadata_dict.update(segment_stats_damage)
                    segment_dict.update(segment_metadata_dict)
                    segment_dict['match_id'] = data_json['data']["attributes"]["id"]
                    player_round_kills.append(segment_dict)

        return (metadata_dict_list, player_round_dict_list, 
                player_round_damage_dict_list, player_summary_dict_list, 
                player_round_kills)
    
    def data_cleaner_guns():

        path_read = 'raw/trackergg/gun_report/'
        path_write = 'cleaned/trackergg/gun_report/'


        files = AwsS3.get_files_list(path_read)

        data = []
        
        for file in files:
            
            file = file.key
            data_s3 = AwsS3.get_file(path_read, file)
            data_json = json.loads(data_s3)
            weapons = data_json['data']

            for weapon in weapons:
                weapon_metadata = weapon["metadata"]
                weapon_stats = weapon["stats"]

                stat_dict = {}
                for stat, stat_data in weapon_stats.items():
                    stat_keys = weapon_stats.keys()
                    stat_columns = [f'{col}' for col in stat_keys]
                    stat_values = weapon_stats.values()
                    _stat_dict = {k: v for k, v in zip(stat_columns, stat_values)}
                    stat_dict.update(_stat_dict)
                
                row = {}
                row.update(weapon_metadata)
                row.update(stat_dict)

                data.append(row)

        df_final = pd.json_normalize(json.loads(json.dumps(data)))

        df_final.to_csv('guns.csv')

        data_final_csv = df_final.to_csv()

        file_format = '.csv'

        AwsS3.upload_file(data_final_csv, path_write, file_format)

servers_list = ['br', 'latam', 'kr', 'na', 'eu', 'ap']
top500_all_servers = Crawler.get_top500_all_servers(servers_list)
df_top500_all_servers = pd.DataFrame(top500_all_servers)
df_top500_all_servers.head(5)
        
players_list = read_csv('data/top500_all_servers.csv')

players_list = players_list['leaderboards.full_nickname'].to_list()

def _remove_duplicates(list_df:list) -> list:
    return list(set(list_df))

unique_players_list = _remove_duplicates(players_list)

top500_matches = Crawler.get_top500_players_matches_report(unique_players_list)

# matches = DataCleaner.data_cleaner_matches()

# matches.to_csv('data/matches_top500_all_servers.csv')