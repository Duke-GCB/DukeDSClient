#!/usr/bin/env bash
CONFIG_FILE=~/.ddsclient
echo "url: http://$DDS_IP:3001/api/v1" > $CONFIG_FILE
echo "user_key: $DDS_USER_KEY" >> $CONFIG_FILE
echo "agent_key: $DDS_AGENT_KEY" >> $CONFIG_FILE
echo "upload_bytes_per_chunk: $UPLOAD_CHUNK_SIZE" >> $CONFIG_FILE
echo "upload_workers: $UPLOAD_WORKERS" >> $CONFIG_FILE


set -e
echo "Installing ddsclient"
eval $TEST_PYTHON setup.py install

echo "Running tests"
$TEST_PYTHON -m unittest discover test_scripts/tests/
echo "HERE"

