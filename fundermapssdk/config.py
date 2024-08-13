from dataclasses import dataclass


@dataclass
class MailConfig:
    """
    MailConfig class represents the configuration for sending emails.

    Attributes:
        api_key (str): The API key for the email service.
        domain (str): The domain of the email service.
        default_sender_name (str): The default name of the sender.
        default_sender_address (str): The default email address of the sender.
    """

    api_key: str
    domain: str
    default_sender_name: str
    default_sender_address: str


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
