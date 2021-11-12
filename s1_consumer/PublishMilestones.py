import datetime
import logging
import os
import boto3
import base64
import json
import random


logger = logging.getLogger()
logger.setLevel(logging.INFO)
TIMEOUT_SQS_URL = os.getenv('TIMEOUT_SQS_URL')
DLQ_SQS_URL = os.getenv('DLQ_SQS_URL')
INFRA_SQS_URL = os.getenv('INFRA_SQS_URL')
error_rate = os.getenv('ERROR_RATE')



sqs = boto3.client('sqs')
client = boto3.client('lambda')


def get_kinesis_records(evt):
    logger.info(f"Get {len(evt['Records'])} Kinesis_records from event")
    records = evt['Records']
    logger.info(f"Processing a batch of {len(records)} records")
    logger.info(f"records type = {type(records)}")
    for r in records:
        validateRecord(r)
    return(records)

def validateRecord(r):
    errs = 0
    payload_byte = (r["kinesis"]["data"]).encode('utf-8')
    print(f"payload = {payload_byte}")
    payloadstr = ((base64.b64decode(payload_byte)).decode('utf-8'))
    pstr = payloadstr.replace("'", '"')
    logger.info(f"**** type={pstr}")
    payload = json.loads(pstr)
    data_type = payload['thing_id']
    payload['retryCount'] = 1
    logger.info(f"Determine processing approach for {data_type}")
    if data_type == "aa-badformat":
        logger.error(f"Bad format error {data_type} in {payloadstr}")
        OutputPayloadToSQS({"retryCount": payload['retryCount'], "Payload": payload}, DLQ_SQS_URL)
        return
    elif data_type == "aa-OK":
        r = random.randint(1,10)
        if r < 3: # simulate a timeout
            logger.error(f"TIMEOUT occurred posting {payload}")
            OutputPayloadToSQS({"retryCount": payload['retryCount'], "Payload": payload}, TIMEOUT_SQS_URL)
        else:
            logger.info(f"good data found {errs}")
            writeSuccessPayload(payload)
        return
    elif data_type ==  "aa-infra":
        logger.info(f"Output to the Infrastructure queue: {payload}")
        OutputPayloadToSQS({"retryCount": payload['retryCount'], "Payload": payload}, INFRA_SQS_URL)

    return(errs)


def OutputPayloadToSQS(msgPayload, sqs_url):
    logger.info(f"Output to SQS {sqs_url}")
    # Send message to SQS queue
    payload = json.dumps(msgPayload)
    response = sqs.send_message(
        QueueUrl=sqs_url,
        DelaySeconds=10,
        MessageBody=json.dumps(payload)
        )
    logger.info(f"Payload = {payload}")
    logger.info(f"SQS Response {response}")
    return response

# TODO Implement output to DynamoDB
def writeSuccessPayload(msgPayload):
    logger.info(f"Outputting message payload to Milestones {msgPayload}")


def lambda_handler(event, context):
    logger.info("===================================================================================================")
    logger.info(f"eeeeeeeeeeeeeeeeeeeeee {event}")
    logger.info("BEGIN Kinesis CONSUMER Lambda Handler")

    records = get_kinesis_records(event)
    logger.info(f"  Records = {type(records)}")
    recordstr = json.dumps(records)
    logger.info(f"recordstr = {recordstr} / {type(recordstr)}")

# TODO Fix this
    response = client.invoke(
           FunctionName='KdsErrProducer',
           InvocationType='Event',
           Payload=recordstr
       )

    print(response)
    logger.info("----------------------------------------------------------------------------------------------------")
    return



