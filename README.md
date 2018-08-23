# DukeDSClient
Command line tool to upload/manage project on the [duke-data-service](https://github.com/Duke-Translational-Bioinformatics/duke-data-service).

[![Build Status](https://travis-ci.org/Duke-GCB/DukeDSClient.svg?branch=master)](https://travis-ci.org/Duke-GCB/DukeDSClient)
[![Coverage Status](https://coveralls.io/repos/github/Duke-GCB/DukeDSClient/badge.svg)](https://coveralls.io/github/Duke-GCB/DukeDSClient)


# Requirements

- [python](https://www.python.org/) - version 2.7+ with a functional ssl module.
- [requests](http://docs.python-requests.org/en/master/) - python module
- [PyYAML](http://pyyaml.org/wiki/PyYAML) - python module

The preferred python versions are 2.7.9+ or 3.4.1+ as they have functional ssl modules by default.
Older python 2.7 may work by following this guide: [Older-python-2.7-setup](https://github.com/Duke-GCB/DukeDSClient/wiki/Older-python-2.7-setup)

# Installation:

DukeDSClient is written in Python and packaged for easy installation from [PyPI](https://pypi.org/project/DukeDSClient/) using `pip`.
If you do not have superuser or administrative privileges on your machine, you will either have to create a [virtual environment (recommended)](https://packaging.python.org/tutorials/installing-packages/#creating-virtual-environments) or run `pip` with the [`--user` scheme](https://docs.python.org/3/install/index.html#alternate-installation-the-user-scheme).

Please see [the tutorial on installing packages](https://packaging.python.org/tutorials/installing-packages/) for full details, but the below commands will create a virtual environment named **ddsclient-env** and install **DukeDSClient**:

```
python3 -m venv ddsclient-env       # Creates an environment called 'ddsclient-env'
source ddsclient-env/bin/activate   # Activates the ddsclient-env environment
pip3 install DukeDSClient           # Installs 'DukeDSClient' into 'ddsclient-env'
```

### Config file setup.

DukeDSClient requires a config file containing an __agent_key__ and a __user_key__.
DukeDSClient supports a global configuration file at /etc/ddsclient.conf and a user configuration file at ~/.ddsclient.
Settings in the user configuration file override those in the global configuration.
Details of all configuration options: [Configuration options](https://github.com/Duke-GCB/DukeDSClient/wiki/Configuration).

#####  Follow these instructions to setup your __user_key__ and  __agent_key__:

[Instructions for adding agent and user keys to the user config file.](https://github.com/Duke-GCB/DukeDSClient/wiki/Agent-User-Keys-(setup))

### Usage:

If DukeDSClient is installed in a [virtual environment](https://packaging.python.org/tutorials/installing-packages/#creating-virtual-environments), you must activate the virtual environment before running ddsclient:

```
source ddsclient-env/bin/activate
```

See general help screen:

```
ddsclient -h
```

See help screen for a particular command:

```
ddsclient <command> -h
```

All commands take the form:
```
ddsclient <command> <arguments...>
```

### Upload:

```
ddsclient upload -p <ProjectName> <Folders/Files...>
```

This will create a project with the name ProjectName in the duke data service for your user if one doesn't exist.
It will then upload the Folders and it's contents to that project.
Any items that already exist with the same hash will not be uploaded.


Example: Upload a folder named 'results' to new or existing project named 'Analyzed Mouse RNA':

```
ddsclient upload -p 'Analyzed Mouse RNA' results
```

### Download:

```
ddsclient download -p <ProjectName> [Folder]
```

This will download the contents of ProjectName into the specified folder.
Currently it requires the directory be empty or not exist.
It will create Folder if it doesn't exist.
If Folder is not specified it will use the name of the project with spaces translated to '_'.

Example: Download the contents of project named 'Mouse RNA' into '/tmp/mouserna' :

```
ddsclient download -p 'Mouse RNA' /tmp/mouserna
```

### Add User To Project:

#### Using duke netid:

```
ddsclient add_user -p <ProjectName> --user <Username> --auth_role 'project_admin'
```

Example: Grant permission to user with username 'jpb123' for a project named 'Analyzed Mouse RNA' with default permissions:

```
ddsclient add_user -p 'Analyzed Mouse RNA' --user 'jpb123'
```

#### Using email:

```
ddsclient add_user -p <ProjectName> --email <Username> --auth_role 'project_admin'
```

Example: Grant permission to user with email 'ada.lovelace@duke.edu' for a project named 'Analyzed Mouse RNA' with default permissions:

```
ddsclient add_user -p 'Analyzed Mouse RNA' --email 'ada.lovelace@duke.edu'
```


### Developer:

Install dependencies:
```
pip install -r devRequirements.txt
```

Setup pre-commit hook:
```
ln pre-commit.sh .git/hooks/pre-commit
```

Run linter/style checker:
```
flake8 --ignore E501 ddsc/
```

Run the tests
```
python setup.py test
```



### Data Service Web Portal:
[Duke Data Service Portal](https://dataservice.duke.edu).
This also requires a [Duke NetID](https://oit.duke.edu/email-accounts/netid/).

### Upload Settings
The default upload settings is to use a worker per cpu and upload 100MB chunks.
You can change this via the `upload_bytes_per_chunk` and `upload_workers` config file options.
These options should be added to your `~/.ddsclient` config file.
`upload_workers` should be an integer for the number of upload workers you want.
`upload_bytes_per_chunk` is the size of chunks to upload. Specify this with MB extension.

Example config file setup to use 4 workers and 200MB chunks:
```
upload_workers: 4
upload_bytes_per_chunk: 200MB
```

### Alternate Service:
The default url is `https://api.dataservice.duke.edu/api/v1`.
You can customize this via the `url` config file option.
Example config file setup to use the __uatest__ server:
```
url: https://apiuatest.dataservice.duke.edu/api/v1
```

You also can specify an alternate url for use with ddsclient via the `DUKE_DATA_SERVICE_URL` environment variable.
Here is how you can set the environment variable so ddsclient will connect to the 'dev' url:
```
export DUKE_DATA_SERVICE_URL='https://apidev.dataservice.duke.edu/api/v1'
```
This will require using the associated portal to get a valid keys.

You will need to specify an `agent_key` and `user_key` in the config file appropriate for the particular service.



