from itgs import Itgs
import traceback


async def handle_error(exc: Exception) -> None:
    """Handles a generic request, potentially outside of the request context"""
    message = "\n".join(
        traceback.format_exception(type(exc), exc, exc.__traceback__)[-5:]
    )
    message = f"```\n{message}\n```"
    print(message)
    async with Itgs() as itgs:
        slack = await itgs.slack()
        await slack.send_web_error_message(message, "an error occurred in backend")
