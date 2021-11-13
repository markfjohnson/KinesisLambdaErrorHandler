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
    messageList = []
    logger.info(f"Number of messages received: {len(response.get('Messages', []))}")
    for message in response.get("Messages", []):
        # Prepare the message body for reposting
        m = message["Body"].replace("'\"","\"")
        msgs = json.loads(m)
        messageList.extend(msgs)
        # Get the receipt handle
        receipt_handle = message['ReceiptHandle']
        # Delete received message from queue
        sqs.delete_message(
            QueueUrl=sqs_url,
            ReceiptHandle=receipt_handle
        )
    logger.info(f"Message List:  {messageList}")
    return messageList


def repostPublisherToMilestoneLambda(messages):
    msg_str = json.dumps(messages)
    response = client.invoke(
        FunctionName='PublishMilestones',
        InvocationType='Event',
        Payload=msg_str
    )
    return(response)

# TODO The retry algorithm just looks at the retrycount for the first row in the list.  This does not work well when
# records from multiple batches are present each with different retry counts.  For production this will require a MUCH
# better algorithm.
def getMessagesFromEvent(evt):
    messages = evt['Records']
    logger.info(f"Messages from trigger {messages} ")
    maxRetryCount = 0
    messageList = []
    for m in messages:
        body = json.loads(m["body"])
        retryCount = body[0].get('retryCount')
        if retryCount is None:
            retryCount = 0
        if maxRetryCount < retryCount:
            maxRetryCount = retryCount

        messageList.extend(body)

    # Put the function to sleep and wait a while before sending the message
    waitTime = maxRetryCount * pauseTime
    time.sleep(waitTime)

    msg_str = json.dumps(messageList)
    response = client.invoke(
        FunctionName='PublishMilestones',
        InvocationType='Event',
        Payload=msg_str
    )
    return (response)

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



