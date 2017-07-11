from __future__ import print_function
import os
import base64
import requests

travis_tag = os.environ['TRAVIS_TAG']
commit_token = os.environ['COMMIT_TOKEN']
commit_user = os.environ['COMMIT_USER']
commit_email = os.environ['COMMIT_EMAIL']

base_url = 'https://api.github.com/repos/Duke-GCB/helmod/contents/rpmbuild/SPECS/'
source_filename = "deploy/ddsclient-generic-gcb01.spec"
target_filename = "ddsclient-{}-gcb01.spec".format(travis_tag)
commit_msg = "DukeDSClient module update {}".format(travis_tag)
commit_branch = "master"

headers = { "Authorization": "token {}".format(commit_token)}
url = '{}{}'.format(base_url, target_filename)

resp = requests.get(url, headers=headers)
if resp.status_code == 404:
    print("Creating spec file {}.".format(target_filename))
    with open(source_filename, 'r') as infile:
        base64_content = base64.b64encode(infile.read().encode('utf-8'))
    data = {
      "path": "rpmbuild/SPECS/{}".format(target_filename),
      "message": commit_msg,
      "committer": {"name": commit_user, "email": commit_email},
      "content": str(base64_content),
      "branch": commit_branch
    }
    resp = requests.put(url, headers=headers, json=data)
    resp.raise_for_status()
else:
    resp.raise_for_status()
    print("Spec file already exists.")
