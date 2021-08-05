import boto3
import json
import base64
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.getenv('TABLE_NAME'))

def lambda_handler(event, context):

    try:
        batch_item_list = []
        for record in event['Records']:
            #Kinesisのレコードはbase64エンコードされているためデコード
            payload = base64.b64decode(record['kinesis']['data'])

            data = json.loads(payload)
            item = {
                'id' : data['id'],
                'timestamp' : int(data['timestamp_ms']),
                'text' : data['text']
            }
            
            batch_item_list.append(item)

        with table.batch_writer() as batch:
            for item in batch_item_list:
                batch.put_item(
                    Item=item
                )
        return

    except Exception as e:
        logging.error("Something went wrong...")
        logging.error(e)
        raise