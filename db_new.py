
from botocore.exceptions import ClientError
from mysql.connector import connect
from mysql.connector import MySQLConnection
from typing import Optional

import boto3
import os
import json

class MysqlDB:
    connection: Optional[MySQLConnection] = None
    params: dict = {}
    secret: dict = {}

    def __init__(self) -> None:
        self.params = {
            'user' : self.get_secret('username'), #'cns_owner', 
            'password' : self.get_secret('password'),
            'host' : self.get_secret('host'),
            'database' : 'callensights'
        }

    def get_connection(self) -> MySQLConnection:
         return connect(**self.params)

    def update_audio_process_status(self, audio, stage, status='R', comments='Started processing..'):
        """
        stage = transcript or analysis
        """
        if stage not in {'transcript', 'analysis'}:
            raise Exception("Invalid stage; stage must be 'transcript' or 'analysis' ")
        
        query_update = f"""
                        UPDATE audio_process_status aps
                        SET {stage} = '{status}',
                            coments = '{comments}'
                        where exists(
                                select 1 
                                from audio_uploads au 
                                where au.audio_id = aps.audio_id
                                and au.file_code = '{audio}')
                        """

        with self.get_connection() as session:
            cur = session.cursor()
            cur.execute(query_update)
            session.commit()
                            
    def is_completed(self, audio_id, stage):
        if stage not in {'transcript', 'analysis'}:
            raise Exception("Invalid stage; stage must be 'transcript' or 'analysis' ")
        
        query_new_audio = f"""
                        SELECT count(*) as cnt
                        FROM audio_uploads au
                        JOIN audio_process_status aps 
                            ON au.audio_id=aps.audio_id
                        WHERE aps.{stage} = 'N'
                        AND au.file_code = '{audio_id}';
                        """
        with self.get_connection() as session:
            cur = session.cursor()
            print("Executing", query_new_audio)
            cur.execute(query_new_audio)
            count, = cur.fetchone()
                
        return count == 0

    def get_secret(self, name:str) -> str:
        
        if name in self.secret:
                print(name, self.secret.get(name))
                return self.secret.get(name)

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
        self.secret =  json.loads(get_secret_value_response['SecretString'])
        return self.secret.get(name)

