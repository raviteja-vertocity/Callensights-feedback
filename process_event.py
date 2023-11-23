from json import dumps
from mongodb import MongoDB

import openai
import os
import logging.handlers

MAX_MESSAGES = 10

# Create logger
logger = logging.getLogger("transcription")
logger.setLevel(logging.INFO)

# Handler
LOG_FILE = "/tmp/transcription-app.log"
handler = logging.handlers.RotatingFileHandler(
    LOG_FILE, maxBytes=1048576, backupCount=5
)
handler.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Add Formatter to Handler
handler.setFormatter(formatter)
logger.addHandler(handler)


def process_event(event_data):
    db = MongoDB()
    try:
        logger.info(f"Processing event: {event_data}")

        user_id = event_data.get("user_id")
        media_code = event_data.get("media_code")

        logger.info(f"Generating feedback for transcript media code: {media_code}")

        transcription = db.get_transcription(media_code=media_code)

        openai.api_key = os.environ.get("OPENAI_API_KEY")
        user_group = db.get_user_group(user_id)
        messages = [db.get_sysmsg(user_group)]
        user_msgs = db.get_usrmsgs(user_group)

        user_msgs.insert(0, {"role": "user", "content": transcription["text"]})

        for msg in user_msgs:
            print("Asking:", msg.get("content"))
            messages.append(msg)
            completion = openai.ChatCompletion.create(
                model="gpt-3.5-turbo", messages=messages[-MAX_MESSAGES:]
            )
            messages.append(completion.choices[0].message)
            print("Response", messages[-1].get("content"))

        # TODO: Store feedback in MongoDB
        feedback = dumps(messages[1:], indent=4)
        updated_feedback = {"media_code": media_code, **feedback}
        # ? WARN: Don't store unless you're sure that the data is completely structured
        # db.put_feedback(feedback=updated_feedback)

    except Exception as e:
        handle_error(f"An unexpected error occurred: {e}", logger)
    finally:
        _cleanup(db, logger)


def handle_error(message):
    """
    Log an error message and raise an exception.

    Parameters:
    - message (str): The error message to log.

    Raises:
    - Exception: Always raises an exception with the specified error message.
    """
    logger.error(message)
    raise Exception(message)


def _cleanup(db: MongoDB):
    """
    Clean up resources after processing an event.

    Parameters:
    - db (MongoDB): MongoDB instance.

    Returns:
    None
    """
    try:
        if db:
            db.client.close()
    except Exception as cleanup_error:
        logger.error(f"An error occurred during cleanup: {cleanup_error}")
