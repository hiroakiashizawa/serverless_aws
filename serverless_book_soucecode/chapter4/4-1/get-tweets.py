from TwitterAPI import TwitterAPI
import credentials
import boto3
import json

consumer_key = credentials.consumer_key
consumer_secret = credentials.consumer_secret
access_token_key = credentials.access_token_key
access_token_secret = credentials.access_token_secret

twitter = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)
kinesis = boto3.client("kinesis")

res = twitter.request("statuses/filter", {"locations":"122.87,24.84,153.01,46.80"})

for item in res:
    print(item['text'])
    kinesis.put_record(StreamName="twitter-stream", Data=json.dumps(item), PartitionKey=item['timestamp_ms'])
