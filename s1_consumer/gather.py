import datetime
import logging
import os
import boto3
import json
import time


logger = logging.getLogger()
logger.setLevel(logging.INFO)
INFRA_SQS_URL = os.getenv('INFRA_SQS_URL')
triggeredInvoke = os.getenv("TRIGGERED")
pauseTime = 30
kinesis_client = boto3.client('kinesis')
client = boto3.client('lambda')
sqs = boto3.client('sqs')

def getSQSrecords(sqs_url):
    logger.info(f"Output from SQS {sqs_url}")
    response = sqs.receive_message(
        QueueUrl = sqs_url,
        MaxNumberOfMessages= 10,
    )
    logger.info(f"Number of messages received: {len(response.get('Messages', []))}")
    for message in response.get("Messages", []):
        message_body = message["Body"]
        logger.info(f"Message body: {json.loads(message_body)}")
        logger.info(f"Receipt Handle: {message['ReceiptHandle']}")
    return response


def repostPublisherToMilestoneLambda(messages):
    response = client.invoke(
        FunctionName='PublishMilestones',
        InvocationType='Event',
        Payload=messages
    )
    print(response)

# TODO The retry algorithm just looks at the retrycount for the first row in the list.  This does not work well when
# records from multiple batches are present each with different retry counts.  For production this will require a MUCH
# better algorithm.
def getMessagesFromEvent(evt):
    messages = evt['Records']
    logger.info(f"Messages from trigger {messages} ")
    retryCount = messages[0].get('retryCount')
    if retryCount is None:
        retryCount = 0
    waitTime = retryCount * pauseTime
    time.sleep(waitTime)
    recordstr = json.dumps(messages)

    return recordstr


def lambda_handler(event, context):
    logger.info("===================================================================================================")
    logger.info(f"eeeeeeeeeeeeeeeeeeeeee {event}")
    logger.info(f"BEGIN Gather-{triggeredInvoke} Lambda Handler")

    if triggeredInvoke == "Invoke":
        messages = getMessagesFromEvent(event)
    else:
        messages = getSQSrecords(INFRA_SQS_URL)
    repostPublisherToMilestoneLambda(messages)
    logger.info("----------------------------------------------------------------------------------------------------")
    return



