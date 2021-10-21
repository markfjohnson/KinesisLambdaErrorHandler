import logging
import os
import boto3
import base64
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)
streamname = os.getenv('STREAM_NAME')
SQS_SuccessQueURL = os.getenv('SUCCESS_SQS_URL')
kinesis_client = boto3.client('kinesis')
print(f"StreamName = {streamname}")

sqs = boto3.client('sqs')

def get_kinesis_records(evt):
    logger.info(f"Get {len(evt['Records'])} Kinesis_records from event")
    records = evt['Records']
    logger.info(f"Processing a batch of {len(records)} records")
    i = 0
    errs = 0
    for r in records:
        if validateRecord(r) is not True:
            errs = errs + 1
        else:
            OutputValidRecords({"validRecordCount": len(evt['Records']), 'Records': records})
        i = i + 1
    return(i, errs)

def validateRecord(r):
    errs = False
    payload_byte = base64.b64decode(r["kinesis"]["data"])
    print(f"payload = {payload_byte}")
    payloadstr = payload_byte.decode('utf8').replace("'", '"')
    logger.info(f"**** type={payloadstr}")
    payload = json.loads(payloadstr)
    data_type = payload['thing_id']
    if data_type == "aa-badformat":
        errs = True
        logger.error(f"Bad Data type {data_type} in {payloadstr}")

    return(errs)

def OutputValidRecords(msgPayload):
    logger.info(f"Output to SQS {SQS_SuccessQueURL}")
    # Send message to SQS queue
    payload = json.dumps(msgPayload)
    response = sqs.send_message(
        QueueUrl=SQS_SuccessQueURL,
        DelaySeconds=10,
        MessageBody=json.dumps(payload)
        )
    return response


def lambda_handler(event, context):
    logger.info("BEGIN Kinesis CONSUMER Lambda Handler")

    (errCount, records) = get_kinesis_records(event)
    if errCount > 0:
        e = f"Bad Input Data Format {errCount}"
        raise Exception(e)
    else:
        status_code = 200

    logger.info(f"END CONSUMER function complete - {status_code}")
    return {
        "statusCode": status_code,
        "body": {
            "records": records
        }
    }


