from json import dumps, loads, load
# from db import MySQLConnector
from db_new import MysqlDB
from sqlalchemy import text
from pathlib import Path

import logging.handlers
import boto3 as aws
import json
import openai
import os


STAGE='analysis'
MAX_MESSAGES = 10
# Create logger
logger = logging.getLogger("feedback")
logger.setLevel(logging.INFO)

# Handler 
LOG_FILE = '/tmp/feedback-app.log'
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1048576, backupCount=5)
handler.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add Formatter to Handler
handler.setFormatter(formatter)

# add Handler to Logger
logger.addHandler(handler)

system_content = """
The Senior Sales Analyst will be responsible for analyzing sales calls, providing constructive feedback to the sales representatives, identifying areas of improvement, and recommending changes to secure closures. The role involves delivering a critical evaluation of sales efforts through a 10-point rating system, aiming at enhancing the overall sales performance.

Key Responsibilities:
Analyze Sales Calls: Systematically review and analyze recorded sales calls to identify key trends, strengths, weaknesses, opportunities, and threats.
Provide Feedback: Offer detailed, actionable feedback to sales representatives, outlining the pros and cons of their approach and suggesting improvements.
Rate Sales Efforts: Implement a 10-point rating system to evaluate sales efforts based on predefined criteria, including communication skills, product knowledge, negotiation techniques, etc.
Develop Improvement Strategies: Collaborate with the sales team to create and implement strategies that drive continuous improvement in sales performance.
Create Reports: Prepare and present analytical reports to the management, showcasing the trends, successes, and areas for enhancement in the sales process.
this is a Conversation between Sales Representative and Potential Customer for Vertocity,
"""

user_msgs = [
    {'role': 'user', 'content': "Give me pros of the pitch"}, 
    {'role': 'user', 'content': "Give me cons of the pitch"},
    {'role': 'user', 'content': "Give me a detailed feedback on what are the areas of improvement."},
    {'role': 'user', 'content': "Give me 10 key metrics and their ratings and overall rating."}
]


def application(environ, start_response):
    logger.info(str(environ))
    path = environ['PATH_INFO']
    method = environ['REQUEST_METHOD']
    if method == 'POST':
        try:
            if path == '/':
                request_body_size = int(environ['CONTENT_LENGTH'])
                request_body = environ['wsgi.input'].read(request_body_size)
                # request_body = b'{"audio_code": "1508202300000014", "audio_bucket": "callensights-audio", "audio_file": "1508202300000014.m4a", "trans_bucket": "callensights-transcript", "trans_file": "1508202300000014.transcript.txt", "analysis_bucket":"callensights-analysis"}'
                logger.info("Received message: %s" % request_body)
            elif path == '/scheduled':
                logger.info("Received task %s scheduled at %s", environ['HTTP_X_AWS_SQSD_TASKNAME'],
                            environ['HTTP_X_AWS_SQSD_SCHEDULED_AT'])
                
            request = loads(request_body)
            db = MysqlDB()
            audio_code = request['audio_code']
                
            if db.is_completed(audio_code, STAGE):
                logger.info(f"Feedback already generated for audio code {audio_code}")
                return
            
            db.update_audio_process_status(audio_code, STAGE)
            try:
                process_event(request)
                db.update_audio_process_status(
                    audio_code,
                    STAGE,
                    status='S', 
                    comments='Transcription Generated Successfully..'
                    )
            except Exception as e:
                logger.error(f'ERROR: {e}')
                db.update_audio_process_status(audio_code, STAGE, status='E', comments=str(e))
                
        
        except (TypeError, ValueError):
            logger.warning('Error retrieving request body for async work.')
            raise
        response = ''
    else:
        response = "welcome"
    start_response("200 OK", [
        ("Content-Type", "text/html"),
        ("Content-Length", str(len(response)))
    ])
    return [bytes(response, 'utf-8')]



def process_event(event_data):
    # Replace this function with your actual event processing logic
    logger.info(f"Processing event: {event_data}")
    trans_file = event_data.get('trans_file')
    trans_bucket = event_data.get('trans_bucket')
    feedback_bucket = event_data.get('analysis_bucket')
    audio_code = event_data.get('audio_code')
    localpath = Path('/tmp')
    local_transcript_file = localpath.joinpath(trans_file)
    feedback_file = audio_code+'.feedback.txt'
    local_feedback_file = localpath.joinpath(feedback_file)

    s3 = aws.client('s3')
    logger.info(f"downloading transcription file to {local_transcript_file}")
    s3.download_file(Bucket=trans_bucket, Key=trans_file, Filename=local_transcript_file)
    try:

        logger.info(f"Generating feedback for transcript {local_transcript_file}")
        with open(local_transcript_file, 'r') as trans_file:
            transcription = load(trans_file)

        openai.api_key = os.environ.get('OPENAI_API_KEY')
        messages = [{'role': 'system', 'content': system_content}]
        
        user_msgs.insert(0, {'role': 'user', 'content': transcription['text']})

        for msg in user_msgs:
            print("Asking:", msg.get('content'))
            messages.append(msg)
            completion = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=messages[-MAX_MESSAGES:]
            )
            messages.append(completion.choices[0].message)
            print("Response", messages[-1].get('content'))
            
        
        logger.info(f'Writing feedback into {local_feedback_file}')
        with open(local_feedback_file, 'w') as fbf:
            fbf.write(json.dumps(messages[1:], indent=4))
            fbf.flush()
        
        logger.info(f"Uploading feedback to {feedback_bucket}/{feedback_file}")
        with open(local_feedback_file, 'r') as lff:
            s3.put_object(Bucket=feedback_bucket, Key=feedback_file, Body=lff.read())
            lff.flush()
        
        logger.debug(f"Deleting teporary files {local_transcript_file} and {local_feedback_file}")
        if local_feedback_file.exists():
            local_feedback_file.unlink()
        if local_transcript_file.exists():
            local_transcript_file.unlink()

    except Exception as e:
        if local_feedback_file.exists():
            local_feedback_file.unlink()
        if local_transcript_file.exists():
            local_transcript_file.unlink()
        raise



if __name__ == '__main__':
    
    input_env = {
        'wsgi.version': (1, 0), 
        'wsgi.multithread': True, 
        'wsgi.multiprocess': False, 
        'wsgi.run_once': False, 
        'wsgi.input_terminated': True, 
        'SERVER_SOFTWARE': 'gunicorn/21.2.0',
        'REQUEST_METHOD': 'POST', 
        'QUERY_STRING': '', 
        'RAW_URI': '/', 
        'SERVER_PROTOCOL': 'HTTP/1.1', 
        'HTTP_CONNECTION': 'upgrade', 
        'HTTP_HOST': 'localhost', 
        'HTTP_X_REAL_IP': '127.0.0.1', 
        'HTTP_X_FORWARDED_FOR': '127.0.0.1', 
        'CONTENT_LENGTH': '153', 
        'CONTENT_TYPE': 'application/json', 
        'HTTP_USER_AGENT': 'aws-sqsd/3.0.4', 
        'HTTP_X_AWS_SQSD_MSGID': '4c3f0f50-4c28-433e-81f8-c9c79bf22f0b', 
        'HTTP_X_AWS_SQSD_RECEIVE_COUNT': '16', 
        'HTTP_X_AWS_SQSD_FIRST_RECEIVED_AT': '2023-08-15T13:57:53Z', 
        'HTTP_X_AWS_SQSD_SENT_AT': '2023-08-15T13:57:53Z', 
        'HTTP_X_AWS_SQSD_QUEUE': 'cns-trans-sqs-dev', 
        'HTTP_X_AWS_SQSD_PATH': '/', 
        'HTTP_X_AWS_SQSD_SENDER_ID': 'AROA4IQUC33B3Q35DSTBL:i-0879d7966a857453a', 
        'HTTP_ACCEPT_ENCODING': 'gzip, compressed', 
        'wsgi.url_scheme': 'http', 
        'REMOTE_ADDR': '127.0.0.1', 
        'REMOTE_PORT': '33380', 
        'SERVER_NAME': '127.0.0.1', 
        'SERVER_PORT': '8000', 
        'PATH_INFO': '/', 
        'SCRIPT_NAME': ''
    }

    # application(input_env, lambda *args, **kwargs: print(*args, **kwargs))