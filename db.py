from botocore.exceptions import ClientError
from mysql.connector import connect
from mysql.connector import MySQLConnection
from typing import Optional, List, Any

import boto3
import os
import json


class MysqlDB:
    connection: Optional[MySQLConnection] = None
    params: dict = {}
    secret: dict = {}
    _STAGE_COLUMNS = {
        'transcript': 'ms_trans_status_cd',
        'analysis': 'ms_fedbk_status_cd'
    }

    def __init__(self, logger) -> None:
        self.logger = logger
        self.params = {
            "user": self.get_secret("username"),  # 'cns_owner',
            "password": self.get_secret("password"),
            "host": self.get_secret("host"),
            "database": "callensights_dev"  # self.get_secret("dbInstanceIdentifier"),
        }

    def get_connection(self) -> MySQLConnection:
        return connect(**self.params)

    def update_audio_process_status(
            self, audio, stage, status="R", comments="Started processing.."
    ):
        """
        stage = transcript or analysis
        """
        self.logger.info(f"Updating {status=} {stage=}")
        column = self._STAGE_COLUMNS.get(stage)
        if not column:
            raise Exception("Invalid stage; stage must be 'transcript' or 'analysis' ")

        query_update = f"""
            UPDATE cns_media_status ms
            SET {column} = %s, 
                ms_comments = %s
            WHERE EXISTS (
                SELECT 1 
                FROM cns_media_def md 
                WHERE ms.ms_media_id = md.cm_media_id
                AND md.cm_media_code = %s
            )
        """

        self.logger.info(f"Running query_update: {query_update}")
        with self.get_connection() as session:
            cur = session.cursor()
            cur.execute(query_update, (status, comments, audio))
            session.commit()

    def is_completed(self, audio_id, stage):
        self.logger.info(f"checking is completed or not {audio_id=}, {stage=}")
        if stage not in {"transcript", "analysis"}:
            raise Exception("Invalid stage; stage must be 'transcript' or 'analysis' ")
        if stage == "transcript":
            column = "ms_trans_status_cd"
        else:
            column = "ms_fedbk_status_cd"

        query_new_audio = f"""
            SELECT COUNT(*) as cnt
            FROM cns_media_def md
            JOIN cns_media_status ms 
                ON ms.ms_media_id = md.cm_media_id
            WHERE ms.{column} NOT IN ('C', 'S')
            AND md.cm_media_code = %s
        """
        self.logger.info(f"{query_new_audio=}")

        self.logger.info(f"Running {query_new_audio} : {audio_id}")
        with self.get_connection() as session:
            cur = session.cursor()
            cur.execute(query_new_audio, (audio_id,))
            (count,) = cur.fetchone()
            self.logger.info(f"{count=}")

        return count == 0

    def get_secret(self, name: str) -> str:
        self.logger.info("Getting Secret")
        if name in self.secret:
            print(name, self.secret.get(name))
            return self.secret.get(name)

        secret_name = os.environ.get(
            "MYSQL_SECRET", "callensights/mysql"
        )  # "dev/callensights/mysql2"
        region_name = os.environ.get("AWS_REGION", "us-east-1")  # "us-east-1"

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)

        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            raise e

        # Decrypts secret using the associated KMS key.
        self.secret = json.loads(get_secret_value_response["SecretString"])
        return self.secret.get(name)

    def get_user_id(self, media_code: str) -> int:
        with self.get_connection() as session:
            query = f"select cm_user_id  from cns_media_def cmd where cm_media_code ='{media_code}'"
            with session.cursor() as cursor:
                cursor.execute(query)
                (user_id,) = cursor.fetchone()

        return user_id

    def get_system_message(self, user_id: int) -> List[Any]:

        with self.get_connection() as session:
            with session.cursor() as cursor:
                cursor.execute(f"select cu_organization, cu_role  from cns_user_def where cu_user_id = {user_id} ")
                (organization, role) = cursor.fetchone()

        context = [
            f"""This is a call transcription between a representative and a prospect customer. the representative is 
            working for {organization} and his role is {role} The resp will speak to multiple customers on a daily 
            basis to sell services of the organization.""", """During the call the rep will try to explain the services of 
            the organization to the customers and resolve any queries that they have. and will try to provide the 
            best buying experience for the customer.""",
            """Your role will be a Sales analyst / call analyst with 10 years of experience in analyzing calls made 
            by sales rep to customers and help to provide best insights and suggestion for the representatives to 
            improve their sales process"""]

        return [{'role': "system", 'content': msg} for msg in context]

    def get_user_message(self) -> List[Any]:
        msgs = [
            {
                'type':"overall_feedback",
                'message': {
                    'role': "user",
                    'content': "Generate feedback for the representative"
                }
            },
            {
                'type':"pros",
                'message': {
                    'role': "user",
                    'content': "Generate Procs for representative in 10 points",
                }
            },
            {
                'type': "cons",
                'message' : {
                    'role': "user",
                    'content': "Generate Cons for representative in 10 points"
                }
            }
        ]

        return msgs

    def get_metric_prompts(self, mcode) -> List[Any]:
        query = f"select stage_desc, metric_prompt from metrics_view mv where media_Code='{mcode}' "
        print(query)
        metrics = []
        with self.get_connection() as session:
            with session.cursor() as cursor:
                cursor.execute(query)
                for (stage_desc,prompt) in cursor.fetchall():
                    metrics.append({
                        'role': 'user',
                        'content': prompt
                    })

        return metrics




if __name__ == "__main__":
    from logging import getLogger

    db = MysqlDB(getLogger("testing"))
    media_code = 'a434b6db-7ea8-4286-94fa-3965ab714785'
    stage = "analysis"
    db.update_audio_process_status(media_code, stage)
    db.update_audio_process_status(media_code, stage, status='R', comments="running")
    db.update_audio_process_status(media_code, stage, status='E', comments="Error")
