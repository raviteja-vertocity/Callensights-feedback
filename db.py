from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from botocore.exceptions import ClientError

import json
import boto3
import os

class MySQLConnector:

    def __init__(self):
        self.username = self.get_secret('username')
        self.password = self.get_secret('password')
        self.host = self.get_secret('host')
        self.port = self.get_secret('port')
        self.database = 'callensights'
        self.engine = self.create_engine()
        self.Session = sessionmaker(bind=self.engine)

    def create_engine(self):
        db_url = f"mysql+mysqlconnector://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        print(db_url)
        engine = create_engine(db_url, pool_pre_ping=True)
        return engine

    def get_connection(self):
        return self.Session()
    
    def update_audio_process_status(self, audio, status='R', comments='Started processing..'):
        query_update = text("""
                            UPDATE audio_process_status
                            SET transcript = :status,
                                coments = :comments
                            where audio_id = :audio
                            """
                            )
        status = {
            'audio': audio,
            'status': status,
            'comments': comments
            }
        with self.get_connection() as session:
            session.execute(query_update, status)
            session.commit()
                            
    def has_transcript(self, audio_id):
        query_new_audio = text("""
                            SELECT count(*) as cnt
                            FROM audio_uploads au
                            JOIN audio_process_status aps 
                                ON au.audio_id=aps.audio_id
                            WHERE aps.transcript = 'N'
                            AND au.file_code = :audio_id;
                            """)
        with self.get_connection() as session:
            result = session.execute(query_new_audio,{'audio_id': audio_id})
            count, = result.fetchone()
            
        return count == 0
            
        
    def get_secret(self, name:str) -> str:

        secret_name = os.environ.get('MYSQL_SECRET', "dev/callensights/mysql2") #"dev/callensights/mysql2"
        region_name = os.environ.get('AWS_REGION', "us-east-1") #"us-east-1"

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            raise e

        # Decrypts secret using the associated KMS key.
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret.get(name)

if __name__ == "__main__":
    # Replace with your MySQL database credentials
    db_username = "your_username"
    db_password = "your_password"
    db_host = "localhost"
    db_port = 3306
    db_name = "your_database_name"

    connector = MySQLConnector()

    # Get a database connection
    session = connector.get_connection()

    # Use the session to perform database operations
    try:
        # Example: Fetch data from a table
        result = session.execute(text("SELECT * FROM audio_upload"))
        for row in result:
            print(row)
    finally:
        session.close()
