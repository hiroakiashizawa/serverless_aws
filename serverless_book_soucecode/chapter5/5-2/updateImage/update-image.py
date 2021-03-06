import boto3
import uuid
import json
import logging
import os
import datetime
import decimal
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table(os.getenv('TABLE_NAME'))

def validate(request_body):
    return request_body.keys() >= {'photo_id', 'timestamp', 'status'}

def lambda_handler(event, context):
    body = json.loads(event['body'])
    if not validate(body):
        error_response = {
            'statusCode': '400',
            'body': 'Validation error',
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin' : '*'
            },
        }
        return error_response
    photo_id = body['photo_id']
    timestamp = body['timestamp']
    status = body['status']
    try:
        try:
            table.update_item(
                Key={'photo_id': photo_id
                     },
                AttributeUpdates={
                  'status': {
                    'Value': status, 
                    'Action': 'PUT'
                  }
                 }
            )
            response = table.get_item(
                Key={'photo_id': photo_id
                     }
            )
        except ClientError as e:
            logging.info(e.response['Error']['Message'])
            response = {
                'statusCode': '400',
                'body': e.response['Error']['Message'],
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin' : '*'
                },
            }
            return response
        else:
            items = json.dumps(response['Item'], cls=DecimalEncoder)
            response = {
                'statusCode': '200',
                'body': items,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin' : '*'
                },
            }
            return response
    except Exception as e:
        logging.error('type: % s', type(e))
        logging.error(e)
