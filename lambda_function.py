import boto3
import csv
import os

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):
    # Get the bucket and object key from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Download the CSV file from S3
    download_path = '/tmp/{}{}'.format(os.path.basename(key), '.csv')
    s3.download_file(bucket, key, download_path)
    
    # Parse and validate the CSV file
    validated_records = []
    with open(download_path, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            # Example validation: Ensure 'name' and 'age' fields exist
            if 'name' in row and 'age' in row:
                # Example: Optionally transform data or perform more complex validation
                validated_records.append(row)
    
    # Store validated records in DynamoDB
    for record in validated_records:
        dynamodb.put_item(
            TableName='your-dynamodb-table-name',
            Item={
                'record_id': {'S': str(record['id'])},  # Assuming 'id' is the primary key in CSV
                'name': {'S': record['name']},
                'age': {'N': str(record['age'])}
                # Add more attributes as needed based on your CSV structure
            }
        )
    
    # Optionally, delete the downloaded file
    os.remove(download_path)
    
    return {
        'statusCode': 200,
        'body': 'Processed {} records.'.format(len(validated_records))
    }
