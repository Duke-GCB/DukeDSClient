#!/usr/bin/env bash
# Circle build trigger should have environment variables COMMIT_USER, COMMIT_EMAIL and COMMIT_TOKEN setup.

pip install requests
python deploy/onpublish.py
