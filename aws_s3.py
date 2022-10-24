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