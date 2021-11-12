import logging
import os
import boto3
import json


logger = logging.getLogger()
logger.setLevel(logging.INFO)
SQS_Queue_URL = os.getenv('SQS_Queue_URL')
tablename = os.getenv("TABLE_NAME")
dynamodb_client = boto3.resource('dynamodb')
logger.info(f"Output table = {tablename}")
table = dynamodb_client.Table(tablename)

sqs = boto3.client('sqs')

def writeMsgToDynamoDB(msg):
    logger.info(f"msg type = {type(msg)} TableName = {tablename}  table={table}")
    response = table.put_item(Item=msg)
    logger.info(f"Wrote {msg} received response = {response}")


def processRecords(msgs):
    logger.info(f"Number of messages received: {msgs}")
    logger.info(f"messages = {msgs}")
    for message in msgs:
        d = json.loads(message['body'])
        logger.info(f"{type(d)} Body = {d}")
        message_body = d.get("Payload")
        logger.info(f"{type(message_body)} Payload ={message_body}")
        if message_body is not None:
            response = writeMsgToDynamoDB(message_body)
            logger.info(response)

    return


def lambda_handler(event, context):
    logger.info("=======================================")
    logger.info(f"++++ Event = {event}")
    records = event.get('Records')
    if records is not None:
        logger.info(f"Received = {records}")
        processRecords(records)
    logger.info("---------------------------------------------------")

    return {
        "statusCode": 200,
        "errorMessage": "Processed messages",
        "body": {
            "records": reqRecs
        }
    }


