import boto3 # Import required for AWS S3 operations


class S3SoapClient:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region):
        """
        Initialize the S3 SOAP client with AWS credentials
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region = region
        self.endpoint = f"https://s3.{region}.amazonaws.com"
        self.namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

        self.session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region
        )
        self.credentials = self.session.get_credentials()


    def get_object(self, bucket_name, object_key):
        """
        Retrieve an object from S3 using boto3
        """
        try:
            s3_client = self.session.client('s3')
            response = s3_client.get_object(
                Bucket=bucket_name,
                Key=object_key
            )
            return response['Body'].read()
        except Exception as exception:
            raise ConnectionError(f"Failed to get object from S3: {exception}")
