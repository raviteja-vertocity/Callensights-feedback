from json import loads

from db import MysqlDB

import logging.handlers

from processor import Processor


STAGE = "analysis"
LOGGING_LEVEL = logging.INFO
LOGGER_NAME = "feedback"

# Create logger
logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(LOGGING_LEVEL)

# Handler
LOG_FILE = "/tmp/feedback-app.log"
handler = logging.handlers.RotatingFileHandler(
    LOG_FILE, maxBytes=1048576, backupCount=5
)
handler.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Add Formatter to Handler
handler.setFormatter(formatter)

# add Handler to Logger
logger.addHandler(handler)

db = MysqlDB()


def application(environ, start_response):
    logger.info(str(environ))
    path = environ["PATH_INFO"]
    method = environ["REQUEST_METHOD"]
    if method == "POST":
        try:
            if path == "/":
                request_body_size = int(environ["CONTENT_LENGTH"])
                request_body = environ["wsgi.input"].read(request_body_size)
                # request_body = b'{"media_code": "0410202300000269", "audio_file": "0410202300000269.mp3", "audio_bucket": "callensights-audio", "trans_bucket": "callensights-transcript", "analysis_bucket": "callensights-analysis", "user_id": "user_2WICvHmUP4iJ8ubmkC4bTbWTZHi", "trans_file": "0410202300000269.transcript.txt"}'
                logger.info("Received message: %s" % request_body)
            elif path == "/scheduled":
                logger.info(
                    "Received task %s scheduled at %s",
                    environ["HTTP_X_AWS_SQSD_TASKNAME"],
                    environ["HTTP_X_AWS_SQSD_SCHEDULED_AT"],
                )

            request = loads(request_body)
            media_code = request["media_code"]

            if db.is_completed(media_code, STAGE):
                logger.info(f"Feedback already generated for audio code {media_code}")
                return

            db.update_audio_process_status(media_code, STAGE)
            try:
                transcription_processor = Processor()
                transcription_processor.process(request)
                db.update_audio_process_status(
                    media_code,
                    STAGE,
                    status="S",
                    comments="Transcription Generated Successfully..",
                )
            except Exception as e:
                logger.error(f"ERROR: {e}")
                db.update_audio_process_status(
                    media_code, STAGE, status="E", comments=str(e)
                )

        except (TypeError, ValueError):
            logger.warning("Error retrieving request body for async work.")
            raise
        response = ""
    else:
        response = "welcome"
    start_response(
        "200 OK",
        [("Content-Type", "text/html"), ("Content-Length", str(len(response)))],
    )
    return [bytes(response, "utf-8")]


if __name__ == "__main__":
    input_env = {
        "wsgi.version": (1, 0),
        "wsgi.multithread": True,
        "wsgi.multiprocess": False,
        "wsgi.run_once": False,
        "wsgi.input_terminated": True,
        "SERVER_SOFTWARE": "gunicorn/21.2.0",
        "REQUEST_METHOD": "POST",
        "QUERY_STRING": "",
        "RAW_URI": "/",
        "SERVER_PROTOCOL": "HTTP/1.1",
        "HTTP_CONNECTION": "upgrade",
        "HTTP_HOST": "localhost",
        "HTTP_X_REAL_IP": "127.0.0.1",
        "HTTP_X_FORWARDED_FOR": "127.0.0.1",
        "CONTENT_LENGTH": "153",
        "CONTENT_TYPE": "application/json",
        "HTTP_USER_AGENT": "aws-sqsd/3.0.4",
        "HTTP_X_AWS_SQSD_MSGID": "4c3f0f50-4c28-433e-81f8-c9c79bf22f0b",
        "HTTP_X_AWS_SQSD_RECEIVE_COUNT": "16",
        "HTTP_X_AWS_SQSD_FIRST_RECEIVED_AT": "2023-08-15T13:57:53Z",
        "HTTP_X_AWS_SQSD_SENT_AT": "2023-08-15T13:57:53Z",
        "HTTP_X_AWS_SQSD_QUEUE": "cns-trans-sqs-dev",
        "HTTP_X_AWS_SQSD_PATH": "/",
        "HTTP_X_AWS_SQSD_SENDER_ID": "AROA4IQUC33B3Q35DSTBL:i-0879d7966a857453a",
        "HTTP_ACCEPT_ENCODING": "gzip, compressed",
        "wsgi.url_scheme": "http",
        "REMOTE_ADDR": "127.0.0.1",
        "REMOTE_PORT": "33380",
        "SERVER_NAME": "127.0.0.1",
        "SERVER_PORT": "8000",
        "PATH_INFO": "/",
        "SCRIPT_NAME": "",
    }

    # application(input_env, lambda *args, **kwargs: print(*args, **kwargs))
