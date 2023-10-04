
from botocore.exceptions import ClientError
from mysql.connector import connect
from mysql.connector import MySQLConnection
from typing import Optional, Any

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
        
        field = 'trans' if stage=='transcript' else 'analysis'
        column = field + ("_start_dt" if status == 'R' else "_end_dt")
        addition = column + " = NOW(), "
        query_update = f"""
                        UPDATE audio_process_status aps
                        SET {addition} {stage} = '{status}',
                            coments = '{comments}'
                        where exists(
                                select 1 
                                from audio_uploads au 
                                where au.audio_id = aps.audio_id
                                and au.file_code = '{audio}')
                        """
        print("Executing:", query_update)
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
                        WHERE aps.{stage} != 'S'
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
                print(name, self.secret.get(name) if name != 'password' else 'XXXXXX')
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
    
    def get_sysmsg(self, group:str) -> dict[str, str]:
        query = f"""
                SELECT group_description
                FROM user_groups
                WHERE group_name = '{group}'
                """
        
        with self.get_connection() as session:
            cur = session.cursor()
            print("Executing:", query)
            cur.execute(query)

            msg, =cur.fetchone()
            return {'role': 'system', 'content': msg}
        
    def get_usrmsgs(self, group:str) -> list[dict[str, str]]:
        query = f"""
            SELECT gm.message
            FROM group_messages gm
            JOIN user_groups g ON (g.group_id=gm.group_id)
            WHERE group_name = '{group}'
            ORDER BY message_sequence
            """
        print("Running:", query)

        with self.get_connection() as session:
            cur = session.cursor()
            cur.execute(query)
            msgs = cur.fetchall()

        return [{'role':'user', 'content':msg} for msg, in msgs]
    
    def get_user_group(self, user_id:str) -> Any:
        query = f"""
            SELECT group_name
            FROM users u
            JOIN user_groups ug ON u.group_id = ug.group_id
            WHERE u.clerk_user_id = '{user_id}'
            """
        print("Getting Group:", query)

        with self.get_connection() as session:
            cur = session.cursor()
            cur.execute(query)
            r = cur.fetchone()

            if not r:
                raise Exception(f"User {user_id}, not assiciated to any Group.")
            grp, = r

        return grp



