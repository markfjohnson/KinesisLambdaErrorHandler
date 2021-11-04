import json
import boto3
import os
import logging
import random
import string

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
streamname = os.getenv('STREAM_NAME')
row_count = int(os.getenv('ROW_COUNT'))
kinesis_client = boto3.client('kinesis')
logger.info("Writing to Kinesis Stream")


def put_to_stream(client, thing_id, property_value, i):
    res = ''.join(random.choices(string.ascii_uppercase +
                                 string.digits, k = 2000))
    # payload = {
    #     'prop': str(property_value),
    #     'iteration': i,
    #     'thing_id': thing_id,
    #     'data': res
    # }
    payload = {
        'prop': str(property_value),
        'iteration': i,
        'thing_id': thing_id,
        "property_value": property_value
    }

    logger.info(f"Before put {payload}")

    put_response = client.put_record(
        StreamName=streamname,
        Data=json.dumps(payload),
        PartitionKey=thing_id)
    return(put_response)

def generate_date(client, row_count):

    i: int = 0
    logger.info("  Starting to generate data")
    for i in range(0, row_count):
        property_value = random.randint(40, 120)
        thing_id_list = ['aa-OK','aa-badformat']
        thing_id = random.choices(thing_id_list, weights=[.8, .2], k=1)[0]
#        thing_id = 'aa-OK'
        logger.info(f"Thing_id = {thing_id}")
        r = put_to_stream(client, thing_id, property_value, i)
        logger.info(f"{i} row put with response = {r}")
        i = i + 1
    return (i, r)



def lambda_handler(event, context):
    """
    Sample Kinesis data producer
    """
    logger.info("Start to put rows into kinesis stream {}".format(streamname))
    logger.info(f"   -- ROWS PER RUN = {row_count}")
    (rows, response) = generate_date(kinesis_client, row_count)
    logger.info("END Producer function complete")
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": {
            "Rowcount": rows,
            "response": response
        }
    }
