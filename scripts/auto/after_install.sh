#!/usr/bin/env bash

if [ ! -d venv ]
then
    python3 -m venv venv
fi
. venv/bin/activate
python -m pip install -U pip
pip install -r requirements.txt
deactivate
