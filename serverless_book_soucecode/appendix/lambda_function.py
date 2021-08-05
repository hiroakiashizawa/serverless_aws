import boto3
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3', region_name = "ap-northeast-1")

def lambda_handler(event, context):
  for record in event['Records']:
    eventName = record['eventName']
    if eventName == "REMOVE":
      key = record['dynamodb']['Keys']['photo_id']['S']
      type = record['dynamodb']['OldImage']['type']['S'].split('/')[1]
      object_key = key + '.' + type

      s3.delete_object(
          Bucket = os.getenv('BUCKET_NAME'),
          Key = object_key
      )
      print('Successfully deleted ' + object_key + '.')       
  return 'Successfully processed {} records.'.format(len(event['Records']))
