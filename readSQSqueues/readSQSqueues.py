import logging
import os
import boto3
import json


logger = logging.getLogger()
logger.setLevel(logging.INFO)
SQS_SuccessQueURL = os.getenv('SUCCESS_SQS_URL')
SQS_ErrorQueueUrL = os.getenv('ERROR_SQS_URL')
SQS_DQLQueueUrl = os.getenv('DLQ_SQS_URL')

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


def lambda_handler(event, context):
    getSQSrecords(SQS_SuccessQueURL)
    getSQSrecords(SQS_ErrorQueueUrL)
    getSQSrecords(SQS_DQLQueueUrl)

    return {
        "statusCode": 200,
        "errorMessage": "Processed messages",
        "body": {
            "records": "record list"
        }
    }
