# DukeDSClient [![CircleCI](https://circleci.com/gh/Duke-GCB/DukeDSClient.svg?style=svg)](https://circleci.com/gh/Duke-GCB/DukeDSClient) [![Coverage Status](https://coveralls.io/repos/github/Duke-GCB/DukeDSClient/badge.svg)](https://coveralls.io/github/Duke-GCB/DukeDSClient)

This command line program will allow you to upload, download, and manage projects in the [duke-data-service](https://github.com/Duke-Translational-Bioinformatics/duke-data-service).

For help email <gcb-help@duke.edu>.


# Requirements

- [python](https://www.python.org/) - version 3.5+

__NOTE:__ When installing Python on Windows be sure to check the `Add Python to PATH` checkbox. This will avoid a problem where `pip3` and/or `ddsclient` cannot be found. 

# Installation:

DukeDSClient can be installed using the `pip3` command line program.

To install or upgrade **DukeDSClient** from a Terminal or Command Prompt run the following:
```
pip3 install --upgrade DukeDSClient
```

The above commmand will install the latest version of DukeDSClient from [PyPI](https://pypi.org/project/DukeDSClient/).

If you receive a permission denied error it may be due to you not having superuser or administrative privileges on your machine. You can run `pip3` with the [`--user` scheme](https://docs.python.org/3/install/index.html#alternate-installation-the-user-scheme) or create a [virtual environment](https://packaging.python.org/tutorials/installing-packages/#creating-virtual-environments) to work around this limitation.
Please see [the tutorial on installing packages](https://packaging.python.org/tutorials/installing-packages/) for more details.

### Config file setup.

DukeDSClient requires a config file containing your credentials used to access the duke-data-service.
Complete details are available in the [configuration documentation](https://github.com/Duke-GCB/DukeDSClient/wiki/Configuration).

#####  Create credentials and config file

[Instructions for adding agent and user keys to the user config file.](https://github.com/Duke-GCB/DukeDSClient/wiki/Agent-User-Keys-(setup))

### Usage:
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

#### Downloading to a mounted file share

To download a project onto a mounted file share (such as a CIFS share) specify a path within the share to download directly there.


### Add User To Project:

#### Using duke netid:

```
ddsclient add-user -p <ProjectName> --user <Username> --auth-role 'project_admin'
```

Example: Grant permission to user with username 'jpb123' for a project named 'Analyzed Mouse RNA' with default permissions:

```
ddsclient add-user -p 'Analyzed Mouse RNA' --user 'jpb123'
```

#### Using email:

```
ddsclient add-user -p <ProjectName> --email <Username> --auth-role 'project_admin'
```

Example: Grant permission to user with email 'ada.lovelace@duke.edu' for a project named 'Analyzed Mouse RNA' with default permissions:

```
ddsclient add-user -p 'Analyzed Mouse RNA' --email 'ada.lovelace@duke.edu'
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



