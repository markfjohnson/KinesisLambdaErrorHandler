import logging
import os
import pprint

import boto3

pp = pprint.PrettyPrinter(indent=4)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info("STARTING")
streamname = os.getenv('STREAM_NAME')
row_count = os.getenv('ROW_COUNT')
kinesis_client = boto3.client('kinesis')
pp = pprint.PrettyPrinter(indent=4)


def list_kinesis_streams(client):
    logger.info("list_kinesis_streams:")
    try:
        r = client.list_streams(Limit=100)
        logger.info("Received stream list")
        pp.pprint(r)
    except Exception as e:
        logger.error(e)


def lambda_handler(event, context):
    print(f"Kinesis_client found {kinesis_client is not None}")
    list_kinesis_streams(kinesis_client)

if __name__ == "__main__":
    print("Local Debug mode")
    list_kinesis_streams(kinesis_client)