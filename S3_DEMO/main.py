import boto3 as boto3
from botocore.exceptions import ClientError
import os
import json

def create_bucket(s3_client, bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param s3_client: connected client
    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        print(e)
        return False
    return True


def list_buckets(s3_client):
    response = s3_client.list_buckets()

    # Output the bucket names
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')


def upload_file(s3_client, file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param s3_client: connected client
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    return True


def download_file(s3_client, bucket_name, object_name):
    with open(object_name, 'wb') as f:
        s3_client.download_fileobj(bucket_name, object_name, f)


def retrieve_policy(s3_client, bucket_name):
    result = s3_client.get_bucket_policy(Bucket=bucket_name)
    print(result['Policy'])


def create_policy(s3_client, bucket_name):
    bucket_policy = {
        'Version': '2024-10-17',
        'Statement': [{
            'Sid': 'AddPerm',
            'Effect': 'Allow',
            'Principal': '*',
            'Action': ['s3:GetObject'],
            'Resource': f'arn:aws:s3:::{bucket_name}/*'
        }]
    }

    # Convert the policy from JSON dict to string
    bucket_policy = json.dumps(bucket_policy)

    # Set the new policy
    s3_client.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)


if __name__ == '__main__':
    # create bucket
    client = boto3.client('s3')
    list_buckets(client)
    #create_bucket(client,"test", "sa-east-1")
