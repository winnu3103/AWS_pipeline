# Real Time Project

_**Data Pipeline Design**_

<img width="560" alt="image" src="https://github.com/winnu3103/real_time_project/assets/135350109/7e6e8509-85b7-4300-9024-63e763ea71a1">

_**Steps Included**_
1. Create a SNS Topic
2. Create a SQS Queue
3. Subscribe the Queue to a SNS topic
4. Create a Lambda fn to convert JSON file in
   raw folder to CSV format and place in format folder
5. Add permissions to IAM role associated with Lambda fn
   for receiving SQS msg, putting files into S3
6. Add SQS queue as trigger source for Lambda fn
7. Create a Event notification for S3 bucket raw folder to send notification
   destined to SNS topic whenever new file inserted into the folder
