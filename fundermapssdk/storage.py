import boto3
import logging

from fundermapssdk.config import S3Config


class ObjectStorageProvider:
    def __init__(self, sdk, config: S3Config):
        self._sdk = sdk
        self.config = config
        self.client = None

    async def upload_file(self, file_path: str, key: str, *args):
        self.__logger(logging.DEBUG, f"Uploading file {file_path} to {key}")

        self.client.upload_file(file_path, self.config.bucket, key, *args)

        self.__logger(logging.DEBUG, f"File uploaded to {key}")

    def __enter__(self):
        # self.__logger(logging.DEBUG, "Connecting to database")

        session = boto3.session.Session()
        self.client = session.client(
            "s3",
            endpoint_url=self.config.service_uri,
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
        )

        # self.__logger(logging.DEBUG, "Connected to database")

        return self

    def __exit__(self, type, value, traceback):
        # self.__logger(logging.DEBUG, "Closing database connection")

        # self.db.close()
        pass

    def __logger(self, level, message):
        return self._sdk._logger.log(level, f"ObjectStorageProvider: {message}")
