import logging
import os
import boto3
import base64
import json
import random



logger = logging.getLogger()
logger.setLevel(logging.INFO)
error_rate = os.getenv('ERROR_RATE')



sqs = boto3.client('sqs')
TIMEOUT_SQS_URL = sqs.get_queue_url(QueueName="TimeoutQueue")['QueueUrl']
DLQ_SQS_URL = sqs.get_queue_url(QueueName="dlqQueue")['QueueUrl']
INFRA_SQS_URL = sqs.get_queue_url(QueueName="infraQueue")['QueueUrl']
client = boto3.client('lambda')
dynamodb = boto3.resource('dynamodb')

def get_kinesis_records(evt):
    logger.info(f"Get {len(evt['Records'])} Kinesis_records from event")
    records = evt['Records']
    logger.debug(f"Processing a batch of {len(records)} records")
    logger.debug(f"records type = {type(records)}")
    for r in records:
        validateRecord(r)
    return(records)

def validateRecord(r):
    errs = 0
    payload_byte = (r["kinesis"]["data"]).encode('utf-8')
    print(f"payload = {payload_byte}")
    payloadstr = ((base64.b64decode(payload_byte)).decode('utf-8'))
    pstr = payloadstr.replace("'", '"')
    logger.debug(f"**** type={pstr}")
    payload = json.loads(pstr)
    data_type = payload['thing_id']
    payload['retryCount'] = 1
    logger.debug(f"Determine processing approach for {data_type}")

    if data_type == "aa-OK":
        r = random.randint(1,10)
        if r < 3: # simulate a timeout
            logger.error(f"TIMEOUT occurred posting {payload}")
            response = OutputPayloadToSQS({"retryCount": payload['retryCount'], "Payload": payload}, TIMEOUT_SQS_URL)
            if response == 200:
                response = 408
            return (response)
        else:
            logger.info(f"good data found {errs}")
            response = writeSuccessPayload(payload)
            return (response)
    elif data_type ==  "aa-infra":
        logger.debug(f"Output to the Infrastructure queue: {payload}")
        response = OutputPayloadToSQS({"retryCount": payload['retryCount'], "Payload": payload}, INFRA_SQS_URL)
        return(response)
    else:
        logger.error(f"Bad format error {data_type} in {payloadstr}")
        response = OutputPayloadToSQS({"retryCount": payload['retryCount'], "Payload": payload}, DLQ_SQS_URL)
        return response

    return(errs)


def writeSuccessPayload(msgPayload):
    logger.info(f"Outputting message payload to Milestones {msgPayload}")

    table = dynamodb.Table('Success')
    if isinstance(msgPayload, str):
        msgPayload = json.loads(msgPayload)
    response = table.put_item(
        Item=msgPayload
    )

    return(response['ResponseMetadata']['HTTPStatusCode'])




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
    logger.debug(f"Message List:  {messageList}")
    return messageList

def OutputPayloadToSQS(msgPayload, sqs_url, delay_seconds=10):
    logger.info(f"Output to SQS {sqs_url}")
    # Send message to SQS queue
    payload = json.dumps(msgPayload)
    response = sqs.send_message(
        QueueUrl=sqs_url,
        DelaySeconds=delay_seconds,
        MessageBody=json.dumps(payload)
    )
    logger.debug(f"Payload = {payload}")
    logger.debug(f"SQS Response {response}")
    return response['ResponseMetadata']['HTTPStatusCode']

#TODO Implement a purge so the test is re-runnable
def purgeSuccessTable():
    return


def getSuccessCount(table):
    records = table.scan()
    return records['Count']

def getTimedOutCount(queue, vis_timeout=0, wait_time=0):
    sqs = boto3.client('sqs')
    sqs_url = sqs.get_queue_url(
        QueueName = queue
    )
    sqs_url = "https://sqs.us-east-1.amazonaws.com/729451883946/TimeoutQueue"
    response = sqs.receive_message(QueueUrl=sqs_url,
                                   MaxNumberOfMessages=1,
                                   MessageAttributeNames=[
                                       'All'
                                   ],
                                   VisibilityTimeout=vis_timeout,
                                   WaitTimeSeconds=wait_time
                                   )
    print(response)
    return(len(response['Messages']))


def get_messages_from_queue(queue_url, recvWaitTime=20):
    sqs_client = boto3.client('sqs')

    messages = []

    while True:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=recvWaitTime
        )

        try:
            messages.extend(resp['Messages'])
        except KeyError:
            break

        entries = [
            {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
            for msg in resp['Messages']
        ]

        resp = sqs_client.delete_message_batch(
            QueueUrl=queue_url, Entries=entries
        )

        if len(resp['Successful']) != len(entries):
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )

    return messages


def lambda_handler(event, context):
    logger.info("===================================================================================================")
    logger.info(f"eeeeeeeeeeeeeeeeeeeeee {event}")
    logger.info("BEGIN Kinesis CONSUMER Lambda Handler")

    records = get_kinesis_records(event)
    logger.info(f"  Records = {type(records)}")
    recordstr = json.dumps(records)
    logger.info(f"recordstr = {recordstr} / {type(recordstr)}")
    logger.info("----------------------------------------------------------------------------------------------------")
    return



