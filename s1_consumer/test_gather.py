import time
import unittest
import gather
import json
import boto3

SQS_URL = "https://sqs.us-east-1.amazonaws.com/729451883946/test_sqs"
sqs = boto3.client('sqs')


def postToSQS(msgPayload, delay_seconds, sqs_url):
    # Send message to SQS queue
    payload = json.dumps(msgPayload)
    response = sqs.send_message(
        QueueUrl=sqs_url,
        DelaySeconds=delay_seconds,
        MessageBody=payload
    )
    return response


class TestGatherCase(unittest.TestCase):

    def test_getSQSrecords(self):
        """Verify get returns just the records passed as part of the setup"""
        a = [{
            "Field1": "AA",
            "iteration": 0
        },
            {
                "Field1": "BB",
                "iteration": 1
            }]
        responses = postToSQS(a, 0, SQS_URL)
        print(responses)
        time.sleep(20)
        messages = gather.getSQSrecords(SQS_URL)
        print(len(messages))
        print(messages)
        assert (len(messages) == 2)

    def test_repostPublisherToMilestoneLambda(self):
        a = [{
            "Field1": "AA",
            "iteration": 0
        },
            {
                "Field1": "BB",
                "iteration": 1
            }]
        response = gather.repostPublisherToMilestoneLambda(a)
        assert (response['StatusCode'] == 202)

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


if __name__ == '__main__':
    unittest.main()
