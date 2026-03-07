"""
Email functionality for FunderMapsSDK.

This module provides email sending capabilities using the Mailgun API.
"""

import logging
from dataclasses import dataclass

from mailgun.client import Client

from fundermapsworker.config import MailConfig

logger = logging.getLogger(__name__)


@dataclass
class Email:
    """
    Email data structure containing all necessary fields for sending an email.

    Attributes:
        to: List of recipient email addresses
        subject: Email subject line
        text: Plain text content of the email
        from_: Optional sender email address (overrides default sender)
    """

    to: list[str]
    subject: str
    text: str
    from_: str | None = None


class MailProvider:
    """
    Provider for sending emails via the Mailgun API.

    This class encapsulates the functionality for configuring and sending emails
    through the Mailgun service.

    Attributes:
        _sdk: Reference to the parent SDK instance
        config: Mail configuration settings
        client: Mailgun client instance
    """
    def __init__(self, sdk, config: MailConfig):
        """
        Initialize the mail provider with SDK reference and configuration.

        Args:
            sdk: The parent SDK instance that contains the logger
            config: Mail configuration containing API key, domain, and sender info
        """
        self._sdk = sdk
        self.config = config
        self.logger = logger
        self.client = Client(auth=("api", config.api_key), api_url=config.base_url)

    def send_simple_message(self, email: Email):
        """
        Send a simple text email message using the Mailgun API.

        This method handles the formatting of the email parameters and sending the request
        to the Mailgun API. It also handles logging and error reporting.

        Args:
            email: An Email object containing recipient, subject, and content information

        Raises:
            Exception: If the email fails to send due to API errors

        Returns:
            None
        """
        self.logger.debug(f"Sending email to {email.to}")

        from_ = (
            email.from_ or f"{self.config.sender_name} <{self.config.sender_address}>"
        )
        to = ", ".join(email.to)

        message_params = {
            "from": from_,
            "to": to,
            "subject": email.subject,
            "text": email.text,
        }

        self.logger.info(f"Email parameters: {message_params}")

        try:
            response = self.client.messages.create(
                domain=self.config.domain, data=message_params
            )

            if response.status_code == 200:
                response_data = response.json()
                message_id = response_data.get("id", "unknown")
                self.logger.debug(f"Email sent successfully: {message_id}")
            else:
                response_data = response.json()
                message = response_data.get("message", "No message provided")
                raise Exception(f"Failed to send email: {message}")
        except Exception as e:
            self.logger.error(f"Failed to send email to {to}: {e}")
            raise
