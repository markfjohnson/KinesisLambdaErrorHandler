import os
from unittest import TestCase, mock
import json
import PublishMilestones

SQS_URL = "https://sqs.us-east-1.amazonaws.com/729451883946/infraQueue"

class MyTestCase(TestCase):

    def test_OutputPayloadToSQS(self):
        msg = [{
            "Field1": "AA",
            "iteration": 0
        },
            {
                "Field1": "BB",
                "iteration": 1
            }]
        response = PublishMilestones.OutputPayloadToSQS(msg, SQS_URL)
        assert (response == 200)
        print(response)

    def test_writeSuccessMessage(self):
        msg = {
            "Field1": "AA",
            "iteration": 0
        }
        response = PublishMilestones.writeSuccessPayload(msgPayload=msg)
        print(response)
        assert (response == 200)

    def test_writeSuccessMessageString(self):
        msg = {
            "Field1": "AA",
            "iteration": 0
        }
        msg = json.dumps(msg)
        response = PublishMilestones.writeSuccessPayload(msgPayload=msg)
        print(response)
        assert (response == 200)


    def test_validateRecordBadFormat(self):
        msg = {
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49574408222142592692164662027912822768781511344925966338",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::999999999999:role/lambda_kinesis",
            "awsRegion": "ap-northeast-1",
            "eventSourceARN": "arn:aws:kinesis:ap-northeast-1:999999999999:stream/test",
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "aa-badformat",
                "sequenceNumber": "49574408222142592692164662027912822768781511344925966338",
                "data": "eyd0aGluZ19pZCc6ICdhYS1iYWRmb3JtYXQnLCAnaXRlcmF0aW9uJzoxLCAndmFsdWUnOidzb21ldGhpbmcnfQ=="
            }
        }

        response = PublishMilestones.validateRecord(msg)
        print(response)
        assert(response == 200)


    def test_validateRecordOkData(self):
        msg = {
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
              "data": "eyd0aGluZ19pZCc6ICdhYS1PSycsICdpdGVyYXRpb24nOjEsICd2YWx1ZSc6J3NvbWV0aGluZyd9"
            }
          }
        response = PublishMilestones.validateRecord(msg)
        print(response)
        assert(response == 200 or response == 408)  ## The OK dataset could post correctly to dynamodb or do a retry and post to a timeout queue

    def test_validateRecordBadInfra(self):
        msg = {
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
        response = PublishMilestones.validateRecord(msg)
        assert(response == 200)
        print(response)


    def test_getRecords(self):
        kinesisEventSource = {'Records': [ {
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
        records = PublishMilestones.get_kinesis_records(kinesisEventSource)
        assert(records is not None)
        assert(len(records) == 3)