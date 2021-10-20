import boto3
import time
import json

payload3=b"""{
}"""
max_rows = 100
client = boto3.client('lambda')
for x in range (0, max_rows):
    response = client.invoke(
        FunctionName='KdsErrProducer',
        InvocationType='Event',
        Payload=payload3
    )
    print(f"Completed {x} runs out of {max_rows}")
    print(response)

#    print(json.loads(response['Payload'].read()))
#    print("\n")
print("Finished")

# TODO Simulate the DLQ
# TODO SAM Template specify DLQ
# TODO Test and understand retry