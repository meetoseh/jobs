from typing import Optional
import aiohttp
import os
import logging
import traceback


class Slack:
    """allows easily sending messages to slack; acts as an
    asynchronous context manager"""

    def __init__(self) -> None:
        self.session: Optional[aiohttp.ClientSession] = None
        """our connection to slack"""

    async def __aenter__(self) -> "Slack":
        assert self.session is None, "enter before exit"
        self.session = aiohttp.ClientSession()
        await self.session.__aenter__()
        return self

    async def __aexit__(self, ex_type, ex_val, ex_tb) -> None:
        assert self.session is not None, "exiting before enter"
        session = self.session
        self.session = None
        await session.__aexit__(ex_type, ex_val, ex_tb)

    async def send_blocks(self, url: Optional[str], blocks: list, preview: str) -> None:
        """sends the given slack formatted block to the given slack url. If the url
        is None, this is a no-op.

        Args:
            url (str): the incoming webhook url
            blocks (list): see https://api.slack.com/messaging/webhooks#advanced_message_formatting
            preview (str): the text for notifications
        """
        assert self.session is not None
        if url is None:
            return
        logging.debug(
            f"slack.send_blocks({url=}, {blocks=}, {preview=})\n{traceback.format_stack()}"
        )
        await self.session.post(
            url=url,
            json={"text": preview, "blocks": blocks},
            headers={"content-type": "application/json; charset=UTF-8"},
        )

    async def send_message(
        self,
        url: Optional[str],
        message: str,
        preview: Optional[str] = None,
        markdown: bool = True,
    ) -> None:
        """sends the given markdown text to the given slack url

        Args:
            url (str, None): the incoming webhook url, or None to do nothing
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

    async def send_oseh_bot_blocks(self, blocks: list, preview: str) -> None:
        """sends the given blocks to the oseh-bot channel

        Args:
            blocks (list): see https://api.slack.com/messaging/webhooks#advanced_message_formatting
            preview (str): the text for notifications
        """

        await self.send_blocks(os.environ.get("SLACK_OSEH_BOT_URL"), blocks, preview)

    async def send_oseh_bot_message(
        self, message: str, preview: Optional[str] = None, markdown: bool = True
    ) -> None:
        """sends the given markdown text to the oseh-bot channel

        Args:
            message (str): the markdown formatted message to send
            preview (str, None): the text for notifications or None to use the message
            markdown (bool): True for markdown format, False for raw text
        """

        await self.send_message(
            os.environ.get("SLACK_OSEH_BOT_URL"), message, preview, markdown
        )

    async def send_oseh_classes_blocks(self, blocks: list, preview: str) -> None:
        """sends the given blocks to the oseh-classes channel

        Args:
            blocks (list): see https://api.slack.com/messaging/webhooks#advanced_message_formatting
            preview (str): the text for notifications
        """
        await self.send_blocks(
            os.environ.get("SLACK_OSEH_CLASSES_URL"), blocks, preview
        )

    async def send_oseh_classes_message(
        self, message: str, preview: Optional[str] = None, markdown: bool = True
    ) -> None:
        """sends the given markdown text to the oseh-classes channel

        Args:
            message (str): the markdown formatted message to send
            preview (str, None): the text for notifications or None to use the message
            markdown (bool): True for markdown format, False for raw text
        """
        await self.send_message(
            os.environ.get("SLACK_OSEH_CLASSES_URL"), message, preview, markdown
        )
