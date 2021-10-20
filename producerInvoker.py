import boto3


payload3=b"""{
}"""
max_rows = 10000
client = boto3.client('lambda')
for x in range (0, max_rows):
    response = client.invoke(
        FunctionName='KdsErrProducer',
        InvocationType='Event',
        Payload=payload3
    )
    print(f"Completed {x} runs out of {max_rows}")
    print(response)


print("Finished")

# TODO Simulate the DLQ
