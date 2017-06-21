#!/usr/bin/env bash
# Travis build trigger should have environment variables COMMIT_USER, COMMIT_EMAIL and COMMIT_TOKEN setup.

SOURCEFILENAME=".deploy/ddsclient-generic-gcb01.spec"
TARGETFILENAME="ddsclient-$TRAVIS_TAG-gcb01.spec"
COMMITMSG="DukeDSClient module update $TRAVIS_TAG"
COMMITBRANCH="master"

# Encode the spec file as base64
BASE64CONTENT=`base64 $SOURCEFILENAME`

# Upload file to helmod repo
curl -i -X PUT -H "Authorization: token $COMMIT_TOKEN" \
   -d "{ \"path\": \"rpmbuild/SPECS/$TARGETFILENAME\", \
        \"message\": \"$COMMITMSG\", \
        \"committer\": {\"name\": \"$COMMIT_USER\", \"email\": \"$COMMIT_EMAIL\"}, \
        \"content\": \"$BASE64CONTENT\", \
        \"branch\": \"$COMMITBRANCH\"}" \
    https://api.github.com/repos/Duke-GCB/helmod/contents/rpmbuild/SPECS/$TARGETFILENAME
