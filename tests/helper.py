import asyncio
import os
import sys
import platform

if platform.system() == "Windows":
    # silences some errors: https://stackoverflow.com/a/68137823
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

curdir = os.getcwd()
if os.path.basename(curdir) == "tests":
    os.chdir(os.path.dirname(curdir))

if "." not in sys.path:
    sys.path.append(".")
