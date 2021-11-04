import datetime
import logging
import os
import boto3
import base64
import json


logger = logging.getLogger()
logger.setLevel(logging.INFO)
SQS_SuccessQueURL = os.getenv('SUCCESS_SQS_URL')
SQS_ErrorQueueUrL = os.getenv('ERROR_SQS_URL')
error_rate = os.getenv('ERROR_RATE')
kinesis_client = boto3.client('kinesis')


sqs = boto3.client('sqs')


def get_kinesis_records(evt):
    logger.info(f"Get {len(evt['Records'])} Kinesis_records from event")
    records = evt['Records']
    logger.info(f"Processing a batch of {len(records)} records")
    i = 0
    errCount = 0
    for r in records:
        errs = validateRecord(r)
        logger.info(f"Validate identified {errs} errors")
        if errs == 1:
            errCount = errCount + 1
            logger.error(f"Incrementing error count {errCount}")
        else:
#            OutputValidRecords({"validRecordCount": len(evt['Records']), 'Records': records}, SQS_SuccessQueURL)
            logger.info(f"Wrote to success queue: {SQS_SuccessQueURL}  total errors={errCount}")
        i = i + 1
    return(i, errCount)

def validateRecord(r):
    errs = 0
    payload_byte = (r["kinesis"]["data"]).encode('utf-8')
    print(f"payload = {payload_byte}")
    payloadstr = ((base64.b64decode(payload_byte)).decode('utf-8'))
    pstr = payloadstr.replace("'", '"')
    logger.info(f"**** type={pstr}")
    payload = json.loads(pstr)
    data_type = payload['thing_id']
    if data_type == "aa-badformat":
        errs = 1
        logger.error(f"Timeout error {data_type} in {payloadstr}")
    else:
        logger.info(f"good data found {errs}")

    return(errs)

def OutputValidRecords(msgPayload, sqs_url):
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
    ts = datetime.datetime.now()
    logger.info("===================================================================================================")
    logger.info(f"**********               {ts}")
    logger.info(f"eeeeeeeeeeeeeeeeeeeeee {event}")
    logger.info("BEGIN Kinesis CONSUMER Lambda Handler")

    (totalRecordCount, errCount) = get_kinesis_records(event)
    if errCount > 0:
        e = f"Bad Input Data Format {errCount} out of {totalRecordCount}"
        raise Exception(e)
    else:
        status_message = "Alls good"
        status_code = 200


    a = {
        "statusCode": status_code,
        "errorMessage": status_message,
        "body": {
            "records": totalRecordCount,
            "errorCount": errCount
        }
    }
    ts = datetime.datetime.now()
    logger.info(f"END CONSUMER function complete - {a}     - Finish time = {ts}")
    logger.info("----------------------------------------------------------------------------------------------------")
    return a



