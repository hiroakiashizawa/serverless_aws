import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

kinesis = boto3.client('kinesis')
cloudwatch = boto3.client('cloudwatch')

def lambda_handler(event, content):
  event_data = json.dumps(event,ensure_ascii=False,indent=4, sort_keys=True, separators=(',', ': '))
  logger.info('Event' + event_data)
  message = json.loads(event['Records'][0]['Sns']['Message'])
  logger.info("Message: " + str(message))

  alarm_name = message['AlarmName']
  stream_name = message['Trigger']['Dimensions'][0]['value']

  if alarm_name == 'kinesis-mon' :

    #現在のオープンシャード数を取得
    stream_summary = kinesis.describe_stream_summary(
      StreamName=stream_name 
    )
    current_open_shard_count = stream_summary['StreamDescriptionSummary']['OpenShardCount']

    #シャードを2倍に変更
    target_shard_count=current_open_shard_count * 2
    responce = kinesis.update_shard_count(
      StreamName=stream_name,
      TargetShardCount=target_shard_count,
      ScalingType='UNIFORM_SCALING'
    )

    #現在のアラームの閾値をシャード数×1000の80％に設定
    new_threshold = target_shard_count*1000*0.8
    logger.info("Set a threshold value to " + new_threshold)
    response = cloudwatch.put_metrics_alerm(
      AlarmName='kinesis_mon',
      MetricName='IncomingRecords',
      Namespace='AWS/Kinesis',
      Period=60,
      EvaluationPeriod=1,
      ComparisonOparator='GreaterThanThreshold',
      Threshold=new_threshold,
      Statistic='Sum'
    )