import boto3
import json
SuccessQueURL = "https://sqs.us-east-1.amazonaws.com/729451883946/SuccessQueue"
sqs = boto3.client('sqs')

def OutputValidRecords(msgPayload):
    # Send message to SQS queue
    att = {
        "A": "abc",
        "B":1
    }
    response = sqs.send_message(
        QueueUrl=SuccessQueURL,
        DelaySeconds=10,
        MessageBody=(json.dumps(att)
        )
    )

    return response

if __name__ == "__main__":
    a = OutputValidRecords("ABC")
    print(a)