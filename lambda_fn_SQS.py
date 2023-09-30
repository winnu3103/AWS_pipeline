import json
import boto3
import csv
import logging
from io import StringIO

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Initialize S3 and SQS clients
    s3_client = boto3.client('s3')
    sqs_client = boto3.client('sqs')

    # Get the SQS message from the Lambda event
    sqs_message = json.loads(event['Records'][0]['body'])
    s3_event = json.loads(sqs_message['Message'])
    logger.info(f's3 event is as follows: {s3_event}')

    # Extract the S3 bucket and object key from the S3 event
    bucket_name = s3_event['Records'][0]['s3']['bucket']['name']
    object_key = s3_event['Records'][0]['s3']['object']['key']
    
    object_key = object_key.replace('raw_folder/', '')

    # Define the source and target file paths
    source_file_path = f'raw_folder/{object_key}'
    target_file_path = f'format_folder/{object_key.replace(".json", ".csv")}'

    logger.info(f'Starting processing of file: {object_key}')

    try:
        # Download the JSON file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=source_file_path)
        json_data = response['Body'].read().decode('utf-8')
        json_array_data = '[' + json_data.replace('}{', '},{') + ']'
        data = json.loads(json_array_data)
        
        # Convert JSON data to CSV
        csv_data = convert_json_to_csv(data)
        
        # Upload the CSV file to S3
        s3_client.put_object(Bucket=bucket_name, Key=target_file_path, Body=csv_data)
        
        logger.info(f'Successfully converted and uploaded {object_key} to {target_file_path}')
        
        return {
            'statusCode': 200,
            'body': 'Conversion and upload to S3 successful.'
        }
    except Exception as e:
        logger.error(f'Error processing file {object_key}: {str(e)}')
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }

def convert_json_to_csv(data):
    logger.info(f'data loaded: {data}')
    # Assuming data is a list of dictionaries with consistent keys
    keys = data[0].keys()
    logger.info(f'Keys are {keys}')
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=keys)
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    return output.getvalue()
