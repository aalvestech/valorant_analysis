from aws_s3 import AwsS3
import pandas as pd
import json
import os
import boto3

class DataCleaner():

    def _remove_duplicates(list_df:list) -> list:
        return list(set(list_df))
    
    def data_cleaner_matches_top500_all_servers():

        path_read = 'raw/matches_report/summary/top500/all_servers/'

        path_write = 'cleaned/matches_report/summary/top500/all_servers/'
        
        df_aux = pd.DataFrame()

        files = AwsS3.get_files_list(path_read)
        

        for file in files:
            file = file.key
            data_aux = []
            data_aux = AwsS3.get_file(path_read, file)
        
        data_s3.append(data_aux)

        return data_s3

    def data_cleaner_guns():

        path_read = 'raw/gun_report/'
        path_write = 'cleaned/gun_report/'


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

    
    def data_cleaner_matches_details_top500_all_servers():
        
        path_read = 'raw/matches_report/details/top500/all_servers/'
        path_write = 'cleaned/matches_report/details/top500/all_servers/'     

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

    def get_matches_ids_offline(matches_path="",local_path=""):
        """
        This function presupposes that a folder containing matches informations exists.
        """
        matches_ids = list()

        for file_name in os.listdir(matches_path):
            match_file = open(f"{matches_path}/{file_name}", encoding="utf8")
            matches_json=json.loads(match_file.read())
            if 'data' in matches_json.keys():
                matches_data = matches_json['data']['matches']
            for match in matches_data:
                matches_ids.append(match['attributes']['id'])

        textfile = open(local_path, "w")
        for element in matches_ids:
            textfile.write(f"{element}\n")
        textfile.close()

        print("DONE!")

    def get_matches_ids_online(path_read : str):
        s3 = boto3.resource('s3')

        files_list = AwsS3.get_files_list(path_read)
        
        matches_ids = list()

        for file in files_list:
            obj = s3.Object(file.bucket_name, file.key)
            body = obj.get()['Body'].read()
            matches_json=json.loads(body)
            if 'data' in matches_json.keys():
                matches_data = matches_json['data']['matches']
            for match in matches_data:
                matches_ids.append(match['attributes']['id'])

        return matches_ids
        