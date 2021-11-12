# Kinesis Lambda Error Handler

## Overview
This project is designed for a hands on exploration of Lambda Error Handling when using the Kinesis Event Source.  At this time the key elements for this project include:
* The s1_producer_errs/producer.py is a utility script which populates messages into Kinesis
* The s1_consumer/consumer.py is a python script which receives data from the Kinesis Event Source.  Within the script there are various data checks to fire off errors.

## Knowledge Tidbits
* You cannot use Dead Letter Queues from the Asynchronous Configuration as the Kinesis Data Source Event Source does not permit the Lambda Dead Letter Queue.  To setup a Dead Letter queue it is necessary to have your program automatically direct messages for retry.
* the raise exception operation in python is necessary to dispay errors in the Lambda functions monitor error chart.  Play around with the raise of exceptions to understand 

## Setup Steps

After you have cloned this repository and reviewed the template.yml (AWS SAM file) and the python source code, then you are ready to deploy the infrastructure to build the infrastructure and populate the Kinesis data stream.  

### Step 1: Build the infrastructure

```sam build & sam deploy --guided```

At this point check out the Lambda Functions and confirm that the functions KdsErrProducer and KdsConsumer are present.  Also, from the Kinesis Data Streams list confirm that there is a new stream which starts with the same name as the stack.

## Step 2: Run the Kinesis Producer

Set the number of rows to push into the Kinesis stream within the source code for s1_producer_errs/producer.py.  The default is 100 rows, which should provide enough data to explore different error situations.
```python producerInvoker.py```
You will see the Kinesis put response as well as the row count display as rows are added.  When the program is done, you will see the message 'Finished' in the console.

## Step 3: Check the error handling
Go into Lambda -> Functions and click on the KdsConsumer function.  From there click on Monitor.  If all successful you will see various metrics.  We are interested in the "Error Count and success rate" monitoring pane, if all is successful at the end of the first producerInvoker you should see errors displayed here.



Play around with various configurations in the template to experiment with different options.



## TODO
Next steps in the project are:
* Add DLQ output from the consumer.py

# DISCOVERY QUESTIONS FOR NAIM
* How long do the Lambda functions take to invoke?

# GOALS
1. Verify duplicate impact
2. 

