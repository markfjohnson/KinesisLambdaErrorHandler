import boto3
import json

def display_payload(payload):
    recs = payload['Records']
    for r in recs:
        print(r['Data'])

def process_sqs_records(msg):
    shardId = msg['shardId']
    startSeqNumber = msg['startSequenceNumber']
    endSeqNumber = msg['endSequenceNumber']
    print("Start", startSeqNumber)
    streamArn = msg['streamArn']
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=stream_name, ShardId=shardId,
                                                       StartingSequenceNumber=startSeqNumber,
                                                       ShardIteratorType="AT_SEQUENCE_NUMBER")
    # shard_iterator = kinesis_client.get_shard_iterator(StreamName=stream_name, ShardId=shardId, ShardIteratorType="TRIM_HORIZON")
    record_response = kinesis_client.get_records(ShardIterator=shard_iterator['ShardIterator'])

    display_payload(record_response)
    while 'NextShardIterator' in record_response:
        record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'])
        if len(record_response['Records']) > 0:
            print(record_response)
            display_payload(record_response)
        else:
            break


kinesis_client = boto3.client('kinesis')
sqs_client = boto3.client('sqs')
queue_url = "https://sqs.us-east-1.amazonaws.com/729451883946/dlqlambda"
stream_name = "LambdaKinesisTest2"

#stream_name = "LambdaKinesisTest2"
#response = client.describe_stream(StreamName=stream_name)
#print(response)
#client.get



# Receive message from SQS queue
response = sqs_client.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=10,
    MessageAttributeNames=[
        'All'
    ],
    VisibilityTimeout=0,
    WaitTimeSeconds=0
)

for m in response['Messages']:
    print(m)
    receipt_handle = m['ReceiptHandle']
    msg =json.loads(m['Body'])['KinesisBatchInfo']
    process_kinesis_records(msg)
    print("======================")


