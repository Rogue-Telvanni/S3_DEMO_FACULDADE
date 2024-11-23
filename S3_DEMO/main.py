import boto3
from botocore.exceptions import ClientError
import os
import json

class S3Helper:
    def __init__(self):
        """Initialize the S3 client."""
        self.s3_client = boto3.client('s3')

    def create_bucket(self, bucket_name, region=None):
        """
        Create an S3 bucket in a specified region.

        :param bucket_name: Name of the bucket to create.
        :param region: AWS region to create the bucket in (default: us-east-1).
        :return: True if bucket created successfully, otherwise False.
        """
        try:
            if region is None:
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                location = {'LocationConstraint': region}
                self.s3_client.create_bucket(Bucket=bucket_name,
                                             CreateBucketConfiguration=location)
            print(f"Bucket '{bucket_name}' created successfully.")
            return True
        except ClientError as e:
            print(f"Error creating bucket '{bucket_name}': {e}")
            return False

    def list_buckets(self):
        """
        List all buckets in the S3 account.
        """
        try:
            response = self.s3_client.list_buckets()
            print("Existing buckets:")
            for bucket in response['Buckets']:
                print(f" - {bucket['Name']}")
        except ClientError as e:
            print(f"Error listing buckets: {e}")

    def delete_bucket(self, bucket_name):
        """
        Delete an S3 bucket.

        :param bucket_name: Name of the bucket to delete.
        """
        try:
            self.s3_client.delete_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' deleted successfully.")
        except ClientError as e:
            print(f"Error deleting bucket '{bucket_name}': {e}")

    def upload_file(self, file_path, bucket_name, object_name=None):
        """
        Upload a file to an S3 bucket.

        :param file_path: Path to the file to upload.
        :param bucket_name: Name of the bucket to upload the file to.
        :param object_name: Name of the object in the bucket (optional).
                            If None, the file name is used.
        :return: True if file was uploaded successfully, otherwise False.
        """
        object_name = object_name or os.path.basename(file_path)
        try:
            self.s3_client.upload_file(file_path, bucket_name, object_name)
            print(f"File '{file_path}' uploaded to bucket '{bucket_name}' as '{object_name}'.")
            return True
        except ClientError as e:
            print(f"Error uploading file '{file_path}': {e}")
            return False

    def download_file(self, bucket_name, object_name, download_path):
        """
        Download a file from an S3 bucket.

        :param bucket_name: Name of the bucket.
        :param object_name: Name of the object to download.
        :param download_path: Path where the downloaded file will be saved.
        """
        try:
            with open(download_path, 'wb') as file:
                self.s3_client.download_fileobj(bucket_name, object_name, file)
            print(f"File '{object_name}' downloaded from bucket '{bucket_name}' to '{download_path}'.")
        except ClientError as e:
            print(f"Error downloading file '{object_name}': {e}")

    def delete_object(self, bucket_name, object_name):
        """
        Delete an object from an S3 bucket.

        :param bucket_name: Name of the bucket.
        :param object_name: Name of the object to delete.
        """
        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=object_name)
            print(f"Object '{object_name}' deleted from bucket '{bucket_name}'.")
        except ClientError as e:
            print(f"Error deleting object '{object_name}': {e}")

    def list_objects(self, bucket_name):
        """
        List all objects in an S3 bucket.

        :param bucket_name: Name of the bucket.
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
            print(f"Objects in bucket '{bucket_name}':")
            if 'Contents' in response:
                for obj in response['Contents']:
                    print(f" - {obj['Key']} (Size: {obj['Size']} bytes)")
            else:
                print("No objects found in the bucket.")
        except ClientError as e:
            print(f"Error listing objects in bucket '{bucket_name}': {e}")

    def get_bucket_policy(self, bucket_name):
        """
        Retrieve and print the bucket policy.

        :param bucket_name: Name of the bucket.
        """
        try:
            result = self.s3_client.get_bucket_policy(Bucket=bucket_name)
            print(f"Bucket policy for '{bucket_name}': {result['Policy']}")
        except ClientError as e:
            print(f"Error retrieving policy for bucket '{bucket_name}': {e}")

    def set_bucket_policy(self, bucket_name):
        """
        Set a bucket policy to allow public read access for all objects in the bucket.

        :param bucket_name: Name of the bucket.
        """
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
        try:
            self.s3_client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(bucket_policy))
            print(f"Public read policy set for bucket '{bucket_name}'.")
        except ClientError as e:
            print(f"Error setting policy for bucket '{bucket_name}': {e}")

if __name__ == '__main__':
    s3_helper = S3Helper()

    bucket_name = "testing-bucket-unisinos"
    region = "sa-east-1"

    # List buckets
    s3_helper.list_buckets()

    # Create a bucket
    s3_helper.create_bucket(bucket_name, region)

    # List buckets again
    s3_helper.list_buckets()

    # Upload a file
    s3_helper.upload_file("test_file.txt", bucket_name)

    # List objects in the bucket
    s3_helper.list_objects(bucket_name)

    # Download the file from the bucket
    s3_helper.download_file(bucket_name, "test_file.txt", "downloaded_test_file.txt")

    # Delete the file from the bucket
    s3_helper.delete_object(bucket_name, "test_file.txt")

    # List objects in the bucket again
    s3_helper.list_objects(bucket_name)

    # Delete the bucket
    s3_helper.delete_bucket(bucket_name)

    # List buckets one final time
    s3_helper.list_buckets()