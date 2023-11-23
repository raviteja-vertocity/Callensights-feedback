from json import dumps
from mongodb import MongoDB
import openai
import os
import logging.handlers

MAX_MESSAGES = 10
LOG_FILE = "/tmp/transcription-app.log"


class Processor:
    """
    The Processor module is designed to process events related to transcription tasks. It interfaces with MongoDB for data storage and retrieval and utilizes the OpenAI GPT-3.5 Turbo model for generating feedback on transcriptions.

    Prerequisites:
        - Python Version: The module is designed to work with Python 3.
        - Dependencies: Install the required dependencies using the following command:
            ```bash
            pip install openai mongodb
            ```
        - OpenAI API Key: Set up an OpenAI API key and export it as an environment variable named `OPENAI_API_KEY`.
        - MongoDB: Configure the MongoDB connection details in the `MongoDB` class.
    """

    MAX_MESSAGES = 10
    LOG_FILE = "/tmp/transcription-app.log"

    def __init__(self):
        """
        Initialize the TranscriptionProcessor.

        Attributes:
            logger (Logger): The logger for recording information and errors.
        """
        self.logger = self._configure_logger()

    def _configure_logger(self):
        """
        Configure the logger.

        Returns:
            Logger: Configured logger.
        """
        logger = logging.getLogger("transcription")
        logger.setLevel(logging.INFO)

        # Handler
        handler = logging.handlers.RotatingFileHandler(
            self.LOG_FILE, maxBytes=1048576, backupCount=5
        )
        handler.setLevel(logging.INFO)

        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Add Formatter to Handler
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

    def process(self, event_data):
        """
        Process an event related to transcription.

        Parameters:
            event_data (dict): Event data containing information about the transcription event.

        Returns:
            None
        """
        db = MongoDB()
        try:
            self.logger.info(f"Processing event: {event_data}")

            user_id = event_data.get("user_id")
            media_code = event_data.get("media_code")

            self.logger.info(
                f"Generating feedback for transcript media code: {media_code}"
            )

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
                    model="gpt-3.5-turbo", messages=messages[-self.MAX_MESSAGES :]
                )
                messages.append(completion.choices[0].message)
                print("Response", messages[-1].get("content"))

            # TODO: Store feedback in MongoDB
            feedback = dumps(messages[1:], indent=4)
            updated_feedback = {"media_code": media_code, **feedback}
            # ? WARN: Don't store unless you're sure that the data is completely structured
            # db.put_feedback(feedback=updated_feedback)

        except Exception as e:
            self.handle_error(f"An unexpected error occurred: {e}")
        finally:
            self._cleanup(db)

    def handle_error(self, message):
        """
        Log an error message and raise an exception.

        Parameters:
            message (str): The error message to log.

        Raises:
            Exception: Always raises an exception with the specified error message.
        """
        self.logger.error(message)
        raise Exception(message)

    def _cleanup(self, db: MongoDB):
        """
        Clean up resources after processing an event.

        Parameters:
            db (MongoDB): MongoDB instance.

        Returns:
            None
        """
        try:
            if db:
                db.client.close()
        except Exception as cleanup_error:
            self.logger.error(f"An error occurred during cleanup: {cleanup_error}")
