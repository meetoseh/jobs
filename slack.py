from typing import Optional
import aiohttp
import os


class Slack:
    """allows easily sending messages to slack; acts as an
    asynchronous context manager"""

    def __init__(self) -> None:
        self.session: Optional[aiohttp.ClientSession] = None
        """our connection to slack"""

    async def __aenter__(self) -> "Slack":
        self.session = aiohttp.ClientSession()
        await self.session.__aenter__()
        return self

    async def __aexit__(self, ex_type, ex_val, ex_tb) -> None:
        session = self.session
        self.session = None
        await session.__aexit__(ex_type, ex_val, ex_tb)

    async def send_blocks(self, url: str, blocks: list, preview: str) -> None:
        """sends the given slack formatted block to the given slack url

        Args:
            url (str): the incoming webhook url
            blocks (list): see https://api.slack.com/messaging/webhooks#advanced_message_formatting
            preview (str): the text for notifications
        """
        await self.session.post(
            url=url,
            json={"text": preview, "blocks": blocks},
            headers={"content-type": "application/json; charset=UTF-8"},
        )

    async def send_message(
        self,
        url: str,
        message: str,
        preview: Optional[str] = None,
        markdown: bool = True,
    ) -> None:
        """sends the given markdown text to the given slack url

        Args:
            url (str): the incoming webhook url
            message (str): the markdown formatted message to send
            preview (str, None): the text for notifications or None to use the message
            markdown (bool): True for markdown format, False for raw text
        """
        if preview is None:
            preview = message
        await self.send_blocks(
            url,
            [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn" if markdown else "plain_text",
                        "text": message,
                    },
                }
            ],
            preview,
        )

    async def send_web_error_blocks(self, blocks: list, preview: str) -> None:
        """sends the given blocks to the web-errors channel

        Args:
            blocks (list): see https://api.slack.com/messaging/webhooks#advanced_message_formatting
            preview (str): the text for notifications
        """

        await self.send_blocks(os.environ.get("SLACK_WEB_ERRORS_URL"), blocks, preview)

    async def send_web_error_message(
        self, message: str, preview: Optional[str] = None, markdown: bool = True
    ) -> None:
        """sends the given markdown text to the web-errors channel

        Args:
            message (str): the markdown formatted message to send
            preview (str, None): the text for notifications or None to use the message
            markdown (bool): True for markdown format, False for raw text
        """

        await self.send_message(
            os.environ.get("SLACK_WEB_ERRORS_URL"), message, preview, markdown
        )

    async def send_ops_blocks(self, blocks: list, preview: str) -> None:
        """sends the given blocks to the ops channel

        Args:
            blocks (list): see https://api.slack.com/messaging/webhooks#advanced_message_formatting
            preview (str): the text for notifications
        """

        await self.send_blocks(os.environ.get("SLACK_OPS_URL"), blocks, preview)

    async def send_ops_message(
        self, message: str, preview: Optional[str] = None, markdown: bool = True
    ) -> None:
        """sends the given markdown text to the ops channel

        Args:
            message (str): the markdown formatted message to send
            preview (str, None): the text for notifications or None to use the message
            markdown (bool): True for markdown format, False for raw text
        """

        await self.send_message(
            os.environ.get("SLACK_OPS_URL"), message, preview, markdown
        )
