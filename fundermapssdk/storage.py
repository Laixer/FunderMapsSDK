import boto3
import logging
from typing import Any
import boto3.session
from boto3.s3.transfer import TransferManager

from fundermapssdk.config import S3Config


class ObjectStorageProvider:
    def __init__(self, sdk, config: S3Config):
        self._sdk = sdk
        self.config = config
        self.client = None

    def upload_file(self, file_path: str, key: str, bucket: None | str = None, *args):
        """
        Uploads a file to the specified key in the storage bucket.

        Args:
            bucket (str): The name of the bucket to upload the file to.
            file_path (str): The path of the file to be uploaded.
            key (str): The key under which to store the file in the bucket.
            *args: Additional arguments to be passed to the upload_file method.

        Returns:
            None

        Raises:
            None
        """
        self.__logger(logging.DEBUG, f"Uploading file {file_path} to {key}")

        self.client.upload_file(file_path, bucket or self.config.bucket, key, *args)

        self.__logger(logging.DEBUG, f"File uploaded to {key}")

    def download_file(self, file_path: str, key: str, bucket: None | str = None, *args):
        """
        Downloads a file from the specified key in the storage bucket.

        Args:
            bucket (str): The name of the bucket to download the file from.
            key (str): The key of the file to be downloaded.
            file_path (str): The path to save the downloaded file to.
            *args: Additional arguments to be passed to the download_file method.

        Returns:
            None

        Raises:
            None
        """
        self.__logger(logging.DEBUG, f"Downloading file {key} to {file_path}")

        self.client.download_file(bucket or self.config.bucket, key, file_path, *args)

        self.__logger(logging.DEBUG, f"File downloaded to {file_path}")

    # TODO: Pass bucket as an argument
    def upload_bulk(
        self, file_paths: str, bucket: None | str = None, extra_args: Any | None = None
    ):
        transfer_manager = TransferManager(self.client)

        for file_path in file_paths:
            transfer_manager.upload(
                file_path,
                bucket or self.config.bucket,
                file_path,
                extra_args=extra_args,
            )

    def __enter__(self):
        self.__logger(logging.DEBUG, "Connecting to S3")

        session = boto3.session.Session()
        self.client = session.client(
            "s3",
            endpoint_url=self.config.service_uri,
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
        )

        self.__logger(logging.DEBUG, "Connected to S3")

        return self

    def __exit__(self, type, value, traceback):
        pass

    def __logger(self, level, message):
        return self._sdk._logger.log(level, f"{self.__class__.__name__}: {message}")
