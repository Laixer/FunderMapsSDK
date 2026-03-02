import logging
from concurrent.futures import ThreadPoolExecutor

import boto3
import boto3.session

from fundermapssdk.config import S3Config

logger = logging.getLogger(__name__)


class ObjectStorageProvider:
    def __init__(self, sdk, config: S3Config):
        self._sdk = sdk
        self.config = config
        self.client = None
        self.logger = logger

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
        self.logger.debug(f"Uploading file {file_path} to {key}")

        self.client.upload_file(file_path, bucket or self.config.bucket, key, *args)

        self.logger.debug(f"File uploaded to {key}")

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
        self.logger.debug(f"Downloading file {key} to {file_path}")

        self.client.download_file(bucket or self.config.bucket, key, file_path, *args)

        self.logger.debug(f"File downloaded to {file_path}")

    def delete_file(self, key: str, bucket: None | str = None):
        """
        Deletes a file from the specified key in the storage bucket.

        Args:
            key (str): The key of the file to be deleted.
            bucket (str, optional): The name of the bucket to delete the file from.
                If None, uses the bucket specified in the config.

        Returns:
            None

        Raises:
            ClientError: If deletion fails due to permissions, connectivity issues,
                         or if the file doesn't exist.
        """
        self.logger.debug(f"Deleting file {key}")

        self.client.delete_object(Bucket=bucket or self.config.bucket, Key=key)

        self.logger.debug(f"File deleted {key}")

    def upload_directory(
        self,
        directory_path: str,
        key: str = "",
        bucket: None | str = None,
        extra_args: dict | None = None,
    ):
        """
        Uploads an entire directory to the storage bucket in parallel.

        This method walks through the specified directory and uploads all files
        while preserving the directory structure. It uses a thread pool to upload
        files concurrently, which can significantly improve performance when
        uploading many files.

        Args:
            directory_path (str): Path to the local directory to upload.
            key (str, optional): The key prefix under which to store files in the bucket.
                If provided, files will be stored under this prefix.
            bucket (str, optional): The name of the bucket to upload files to.
                If None, uses the bucket specified in the config.
            extra_args (dict, optional): Extra arguments to pass to the upload operation,
                such as ContentType, ACL, etc.

        Returns:
            None

        Raises:
            Exception: If not all files were successfully uploaded.
        """
        import os
        import threading

        file_paths = []
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_paths.append(os.path.join(root, file))

        self._upload_count = 0
        self._upload_count_lock = threading.Lock()

        def _upload_file(local_path):
            rel_path = os.path.relpath(local_path, directory_path)
            s3_key = os.path.join(key, rel_path).replace("\\", "/")

            self.client.upload_file(
                local_path, bucket or self.config.bucket, s3_key, extra_args
            )
            with self._upload_count_lock:
                self._upload_count += 1

        MAX_THREADS = 10
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            executor.map(_upload_file, file_paths)

        if self._upload_count != len(file_paths):
            raise Exception("Failed to upload all files")

        self.logger.debug(
            f"Uploaded {self._upload_count} files from directory {directory_path}",
        )

    def __enter__(self):
        """
        Context manager entry point. Initializes S3 client connection.

        Returns:
            ObjectStorageProvider: The initialized storage provider instance.
        """
        self.logger.debug("Connecting to S3")

        session = boto3.session.Session()
        self.client = session.client(
            "s3",
            endpoint_url=self.config.service_uri,
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
        )

        self.logger.debug("Connected to S3")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
