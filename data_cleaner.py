from aws_s3 import AwsS3
import pandas as pd
import json


class DataCleaner():

    def _remove_duplicates(list_df:list) -> list:
        return list(set(list_df))
    
    def data_cleaner_matches():

        path_read = 'raw/trackergg/matches_report/'
        path_write = 'cleaned/trackergg/matches_report/'
        
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

        df_final.to_csv('matches.csv')

        data_final_csv = df_final.to_csv()

        file_format = '.csv'

        AwsS3.upload_file(data_final_csv, path_write, file_format)

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

    
    def data_cleaner_matches_details():
        
        path_read = 'raw/trackergg/matches_report_details/'
        path_write = 'cleaned/trackergg/matches_report_details/'     

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