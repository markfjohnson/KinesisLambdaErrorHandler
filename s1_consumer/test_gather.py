import time
from unittest import TestCase

import boto3

from lib.aws_utility import purgeSuccessTable
from lib.aws_utility import getSuccessCount
from lib.aws_utility import getTimedOutCount
from lib.aws_utility import get_messages_from_queue
import gather

SQS_URL = "https://sqs.us-east-1.amazonaws.com/729451883946/test_sqs"

sqs = boto3.client('sqs')
TIMEOUT_URL = sqs.get_queue_url(QueueName="TimeoutQueue")['QueueUrl']
dynamodb = boto3.resource('dynamodb')
success_table = dynamodb.Table('Success')



class TestGatherCase(TestCase):


    def setUp(self):
        purgeSuccessTable()

    def test_get_messages_from_queue(self):
        messages = get_messages_from_queue(TIMEOUT_URL)
        print(messages)
        assert(messages.get('Messages') is not None)
        assert(len(messages.get('Messages')) == 2)


    def test_countSQSqueue(self):
        cnt = getTimedOutCount('TimeoutQueue')
        assert(cnt==5)


    def test_getSuccessCount(self):
        assert (1==2)


    def test_repostPublisherToMilestoneLambda(self):
        repostRecords = {'Records': [ {
             "eventSource": "aws:kinesis",
              "eventVersion": "1.0",
              "eventID": "shardId-000000000000:49574408222142592692164662027912822768781511344925966338",
              "eventName": "aws:kinesis:record",
              "invokeIdentityArn": "arn:aws:iam::999999999999:role/lambda_kinesis",
              "awsRegion": "ap-northeast-1",
              "eventSourceARN": "arn:aws:kinesis:ap-northeast-1:999999999999:stream/test",
            "kinesis": {
              "kinesisSchemaVersion": "1.0",
              "partitionKey": "aa-OK",
              "sequenceNumber": "49574408222142592692164662027912822768781511344925966338",
              "data": "eyd0aGluZ19pZCc6ICdhYS1pbmZyYScsICdpdGVyYXRpb24nOjMsICd2YWx1ZSc6J2JhZCBwZXJtaXNzaW9ucyd9"
            }
          },
            {
                "eventSource": "aws:kinesis",
                "eventVersion": "1.0",
                "eventID": "shardId-000000000000:49574408222142592692164662027912822768781511344925966338",
                "eventName": "aws:kinesis:record",
                "invokeIdentityArn": "arn:aws:iam::999999999999:role/lambda_kinesis",
                "awsRegion": "ap-northeast-1",
                "eventSourceARN": "arn:aws:kinesis:ap-northeast-1:999999999999:stream/test",
                "kinesis": {
                    "kinesisSchemaVersion": "1.0",
                    "partitionKey": "aa-OK",
                    "sequenceNumber": "49574408222142592692164662027912822768781511344925966338",
                    "data": "eyd0aGluZ19pZCc6ICdhYS1pbmZyYScsICdpdGVyYXRpb24nOjMsICd2YWx1ZSc6J2JhZCBwZXJtaXNzaW9ucyd9"
                }
            },
            {
                "eventSource": "aws:kinesis",
                "eventVersion": "1.0",
                "eventID": "shardId-000000000000:49574408222142592692164662027912822768781511344925966338",
                "eventName": "aws:kinesis:record",
                "invokeIdentityArn": "arn:aws:iam::999999999999:role/lambda_kinesis",
                "awsRegion": "ap-northeast-1",
                "eventSourceARN": "arn:aws:kinesis:ap-northeast-1:999999999999:stream/test",
                "kinesis": {
                    "kinesisSchemaVersion": "1.0",
                    "partitionKey": "aa-OK",
                    "sequenceNumber": "49574408222142592692164662027912822768781511344925966338",
                    "data": "eyd0aGluZ19pZCc6ICdhYS1pbmZyYScsICdpdGVyYXRpb24nOjMsICd2YWx1ZSc6J2JhZCBwZXJtaXNzaW9ucyd9"
                }
            }
        ]
        }
        gather.repostPublisherToMilestoneLambda(repostRecords)
        time.sleep(120)
        sucCount = getSuccessCount(success_table)
        print("Success Count", sucCount)
        assert(sucCount == 3)

        # Loop through get messages in dynamodb until all 3 records received






    def test_getMessagesFromEvent(self):
        eventSource = {
            "Records": [
                {
                    "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
                    "receiptHandle": "MessageReceiptHandle",
                    "body": "[{\"Field1\": \"AA\", \"iteration\": 0}, {\"Field1\": \"BB\", \"iteration\": 1}]",
                    "attributes": {
                        "ApproximateReceiveCount": "1",
                        "SentTimestamp": "1523232000000",
                        "SenderId": "123456789012",
                        "ApproximateFirstReceiveTimestamp": "1523232000001"
                    },
                    "messageAttributes": {},
                    "md5OfBody": "{{{md5_of_body}}}",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:MyQueue",
                    "awsRegion": "us-east-1"
                },
                {
                    "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
                    "receiptHandle": "MessageReceiptHandle",
                    "body": "[{\"Field1\": \"CC\", \"iteration\": 2, \"retryCount\":1}, {\"Field1\": \"DD\", \"iteration\": 3, \"retryCount\":1}]",
                    "attributes": {
                        "ApproximateReceiveCount": "1",
                        "SentTimestamp": "1523232000000",
                        "SenderId": "123456789012",
                        "ApproximateFirstReceiveTimestamp": "1523232000001"
                    },
                    "messageAttributes": {},
                    "md5OfBody": "{{{md5_of_body}}}",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:MyQueue",
                    "awsRegion": "us-east-1"
                }
            ]
        }

        result = gather.getMessagesFromEvent(eventSource)
        assert(result['StatusCode']==202)



