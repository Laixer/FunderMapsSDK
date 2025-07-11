from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """
    DatabaseConfig class represents the configuration for connecting to a database.

    Attributes:
        database (str): The name of the database.
        host (str): The host of the database.
        user (str): The user for the database.
        password (str): The password for the database.
        port (int): The port for the database.
    """

    database: str
    host: str
    user: str
    password: str
    port: int


@dataclass
class S3Config:
    """
    S3Config class represents the configuration for connecting to an S3 bucket.

    Attributes:
        bucket (str): The name of the S3 bucket.
        access_key (str): The access key for the S3 bucket.
        secret_key (str): The secret key for the S3 bucket.
        service_uri (str): The service URI for the S3 bucket
    """

    bucket: str
    access_key: str
    secret_key: str
    service_uri: str


@dataclass
class PDFCoConfig:
    """
    PDFCoConfig class represents the configuration for the PDF service.

    Attributes:
        api_key (str): The API key for the PDF service.
    """

    api_key: str


@dataclass
class MailConfig:
    """
    MailConfig class represents the configuration for the Mailgun service.

    Attributes:
        api_key (str): The API key for Mailgun.
        domain (str): The Mailgun domain to send emails from.
        base_url (str): The base URL for the Mailgun API.
        sender_name (str): The default sender name for emails sent through Mailgun.
        sender_address (str): The default sender email address for emails sent through Mailgun.
    """

    api_key: str
    domain: str
    base_url: str
    sender_name: str
    sender_address: str
