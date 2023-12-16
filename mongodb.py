from pymongo import MongoClient
from botocore.exceptions import ClientError
import json
import boto3
import os


class MongoDB:
    """
    MongoDB Connector class for inserting transcriptions.
    """

    def __init__(self, database="callensights"):
        self.database = database
        self.secret = None
        self.client = None
        self.boto_session = boto3.session.Session()
        self.boto_client = self.boto_session.client(
            service_name="secretsmanager",
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
        )

    def get_connection(self):
        """
        Get MongoDB connection using the provided credentials.
        """
        if True:
            mongo_url = f"mongodb+srv://{self.get_secret('username')}:{self.get_secret('password')}@{self.get_secret('host')}/?retryWrites=true&w=majority"
            self.client = MongoClient(mongo_url)
        return self.client

    def put_feedback(self, feedback, collection_name="feedbacks") :
        """
        Insert feedback data into MongoDB.
        """
        with self.get_connection() as client:
            db = client[self.database]
            collection = db[collection_name]
            response = collection.insert_one(feedback)
        return response

    def get_transcription(self, media_code, collection_name="transcriptions") -> str:
        """
        Get transcription data from the MongoDB.
        """
        with self.get_connection() as client:
            db = client[self.database]
            collection = db[collection_name]
            return collection.find_one({"media_code": str(media_code)})

    def get_secret(self, name: str) -> str:
        """
        Retrieve secrets from AWS Secrets Manager.
        """
        if self.secret and name in self.secret:
            return self.secret.get(name)

        secret_name = os.environ.get("MONGO_SECRET", "callensights/mongodb")

        try:
            get_secret_value_response = self.boto_client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise Exception(f"Failed to retrieve secret: {e}") from e

        self.secret = json.loads(get_secret_value_response["SecretString"])
        return self.secret.get(name)


if __name__ == "__main__":
    # Example usage
    media_code = "92739123"

    connector = MongoDB()

    try:
        result = connector.get_transcription(media_code=media_code)
        print(result)
        # print(result['text'])
    finally:
        connector.client.close()
