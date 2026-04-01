#!/bin/bash
# Start ssh server
service ssh restart

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install venv-pack
venv-pack -f -o .venv.tar.gz

# Prepare data
python3 prepare_data.py

# Create index and store to DB
bash index.sh

# Demo Search
echo "Query 1: 'data collection python'"
bash search.sh "data collection python"

echo "Query 2: 'distributed systems mapreduce'"
bash search.sh "distributed systems mapreduce"