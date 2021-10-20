import logging
import os
import boto3
import base64
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)
streamname = os.getenv('STREAM_NAME')
row_count = os.getenv('ROW_COUNT')
kinesis_client = boto3.client('kinesis')
print(f"StreamName = {streamname}")
max_records = 50

def get_kinesis_records(evt):
    logger.info(f"Get {len(evt['Records'])} Kinesis_records from event")
    records = evt['Records']
    logger.info(f"Processing a batch of {len(records)} records")
    i = 0
    errs = 0
    for r in records:
        payload_byte=base64.b64decode(r["kinesis"]["data"])
        payloadstr = payload_byte.decode('utf8').replace("'", '"')
        logger.info(f"{i} The record length is {len(payloadstr)}")
        logger.info(f"    type={payloadstr}")
        payload = json.loads(payloadstr)
        data_type = payload['thing_id']
        if data_type == "aa-badformat":
            errs = errs + 1
        logger.info(f"Data = {payload['thing_id']}")
        i = i + 1
    return(i, errs, records)




def lambda_handler(event, context):
    logger.info("BEGIN Kinesis CONSUMER Lambda Handler")
    rcount = 0
    errCount = 0
    (rcount, errCount, records) = get_kinesis_records(event)
    if errCount > 0:
        raise Exception("Bad Input Data Format")
    else:
        status_code = 200

    logger.info(f"END CONSUMER function complete - {status_code}")
    return {
        "statusCode": status_code,
        "body": {
            "records": records
        }
    }


