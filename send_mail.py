import asyncio
import argparse

from fundermapssdk.mail import Email
from fundermapssdk.command import FunderMapsCommand


class SendMailCommand(FunderMapsCommand):
    def __init__(self):
        super().__init__(description="Send email functionality")

    def add_arguments(self, parser: argparse.ArgumentParser):
        """Add command-line arguments for the command."""
        parser.add_argument(
            "--to", type=str, required=True, help="Recipient email address"
        )
        parser.add_argument("--subject", type=str, required=True, help="Email subject")
        parser.add_argument("--text", type=str, required=True, help="Email body text")

    async def execute(self):
        """Execute the email sending command."""
        if (
            hasattr(self.args, "to")
            and hasattr(self.args, "subject")
            and hasattr(self.args, "text")
        ):
            email = Email(
                to=[self.args.to],
                subject=self.args.subject,
                text=self.args.text,
            )
            self.fundermaps.mail.send_simple_message(email)
            print(f"Email sent to {self.args.to} with subject '{self.args.subject}'")
            return 0
        else:
            print("Missing required arguments: --to, --subject, --text")
            return 1


if __name__ == "__main__":
    exit_code = asyncio.run(SendMailCommand().run())
    exit(exit_code)
