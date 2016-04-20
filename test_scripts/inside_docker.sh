#!/usr/bin/env bash
CONFIG_FILE=~/.ddsclient
echo "url: http://$DDS_IP:3001/api/v1" > $CONFIG_FILE
echo "user_key: $DDS_USER_KEY" >> $CONFIG_FILE
echo "agent_key: $DDS_AGENT_KEY" >> $CONFIG_FILE
echo "upload_bytes_per_chunk: $UPLOAD_CHUNK_SIZE" >> $CONFIG_FILE
echo "upload_workers: $UPLOAD_WORKERS" >> $CONFIG_FILE

set -e
eval $TEST_PYTHON setup.py install

#see if we can upload and download a small file
PROJECT_NAME="small$TEST_PYTHON"
DOWNLOAD_DIR=/tmp/small

ddsclient upload -p $PROJECT_NAME setup.py
ddsclient download -p $PROJECT_NAME $DOWNLOAD_DIR
ls /tmp/DukeDSClientData/*
diff setup.py $DOWNLOAD_DIR/setup.py

eval $TEST_PYTHON api_test.py get_projects | grep desc



