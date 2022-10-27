import os
import sys


curdir = os.getcwd()
if os.path.basename(curdir) == "tests":
    os.chdir(os.path.dirname(curdir))

if "." not in sys.path:
    sys.path.append(".")
