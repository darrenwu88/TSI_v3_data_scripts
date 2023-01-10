### Test script for uploading .csv data to AWS S3 bucket

#key = AKIAQ5Z5HTBQUWFYKRUG
#pass = u9H9IwB1ilvpsmxKmas8LY+Up41Vw0uzddBoVzJW
import boto3
import os

#client credentials set up on AWS website
client = boto3.client('s3', 
                        aws_access_key_id = 'AKIAQ5Z5HTBQUWFYKRUG',
                        aws_secret_access_key = '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')

#Path for csv files
PATH = '/Users/darrenwu/TSI_v3_data_scripts/flat_telemetry_csv_RAW'

#uploading all csv files in PATH dir
for file in os.listdir(PATH):
    #bucket name
    upload_file_bucket = 'v3-archived-data'
    #files being uploaded into /test/ folder
    upload_file_key = 'test/' + str(file)
    #upload function
    client.upload_file(PATH + '/' + file, upload_file_bucket, upload_file_key)