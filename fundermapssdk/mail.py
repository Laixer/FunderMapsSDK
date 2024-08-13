import httpx
import logging
from dataclasses import dataclass

from fundermapssdk.config import MailConfig


@dataclass
class Email:
    to: list[str]
    subject: str
    text: str
    from_: str | None = None


class MailProvider:
    def __init__(self, sdk, config: MailConfig):
        self._sdk = sdk
        self.config = config

    async def send_simple_message(self, email: Email):
        self.__logger(logging.DEBUG, f"Sending email to {email.to}")

        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.eu.mailgun.net/v3/{self.config.domain}/messages",
                auth=("api", self.config.api_key),
                data={
                    "from": email.from_
                    or f"{self.config.default_sender_name} <{self.config.default_sender_address}>",
                    "to": email.to,
                    "subject": email.subject,
                    "text": email.text,
                },
            )

        self.__logger(logging.INFO, f"Email sent to {email.to}")

    def __logger(self, level, message):
        return self._sdk._logger.log(level, f"MailProvider: {message}")
