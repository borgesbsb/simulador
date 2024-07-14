#!/bin/bash

python3 -m virtualenv venv

source venv/bin/activate

pip install -r requirements.txt

streamlit run app.py --server.maxMessageSize 4000

deactivate
