import unittest
import consumer
import boto3


class MyTestCase(unittest.TestCase):
    def test_OutputValidRecords(self):
        sqs = boto3.client('sqs')
        payload = {'A':'Hello','B':2}
        response = consumer.OutputValidRecords(payload, "https://sqs.us-east-1.amazonaws.com/729451883946/SuccessQueue1")
        print(response)

        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)  # add assertion here


if __name__ == '__main__':
    unittest.main()
