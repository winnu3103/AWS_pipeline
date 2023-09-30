import boto3
import json
import csv
from io import StringIO
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Set your source and target bucket names
    source_bucket = 'demo-bucket-json-concatenator'
    target_bucket = 'demo-bucket-json-concatenator'  
    source_folder = 'raw_folder/' 
    target_folder = 'format_folder/'

    s3_client = boto3.client('s3')

    for record in event['Records']:
        # Get the S3 object key from the event
        s3_key = record['s3']['object']['key']

        # Check if the object is within the specified source folder
        if not s3_key.startswith(source_folder):
            continue

        # Build the target key by replacing the source folder with the target folder
        target_key = target_folder + s3_key[len(source_folder):].replace('.json', '.csv')
        
        logger.info(f'Target Key: {target_key}')

        # Read the JSON object from S3 as plain text
        response = s3_client.get_object(Bucket=source_bucket, Key=s3_key)
        json_text = response['Body'].read().decode('utf-8')

        # Add commas and square brackets to make it a valid JSON array
        json_array_text = '[' + json_text.replace('}{', '},{') + ']'
        json_data = json.loads(json_array_text)

        # Convert JSON to CSV
        csv_data = StringIO()
        csv_writer = csv.DictWriter(csv_data, fieldnames=json_data[0].keys())
        csv_writer.writeheader()
        for row in json_data:
            csv_writer.writerow(row)

        # Upload the CSV to the target S3 bucket with the target key
        s3_client.put_object(Bucket=target_bucket, Key=target_key, Body=csv_data.getvalue())

    return {
        'statusCode': 200,
        'body': json.dumps('Conversion complete.')
    }
