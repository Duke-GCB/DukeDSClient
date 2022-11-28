# DukeDSClient [![CircleCI](https://circleci.com/gh/Duke-GCB/DukeDSClient.svg?style=svg)](https://circleci.com/gh/Duke-GCB/DukeDSClient) [![Coverage Status](https://coveralls.io/repos/github/Duke-GCB/DukeDSClient/badge.svg)](https://coveralls.io/github/Duke-GCB/DukeDSClient)

This command line program (`ddd`) will allow you to upload, download, and manage projects in the [DHTS Storage as a Service](https://azurestorage.duhs.duke.edu/). Previously there was a `ddsclient` command line tool that is now deprecated.

For help email <gcb-help@duke.edu>.


# Requirements

- [python](https://www.python.org/) - version 3.7+

__NOTE:__ When installing Python on Windows be sure to check the `Add Python to PATH` checkbox. This will avoid a problem where `pip3` and/or `dds` cannot be found. 

# Installation:

DukeDSClient can be installed using the `pip3` command line program.

To install or upgrade **DukeDSClient** from a Terminal or Command Prompt run the following:
```
pip3 install --upgrade DukeDSClient
```

The above commmand will install the latest version of DukeDSClient from [PyPI](https://pypi.org/project/DukeDSClient/).

If you receive a permission denied error it may be due to you not having superuser or administrative privileges on your machine. You can run `pip3` with the [`--user` scheme](https://docs.python.org/3/install/index.html#alternate-installation-the-user-scheme) or create a [virtual environment](https://packaging.python.org/tutorials/installing-packages/#creating-virtual-environments) to work around this limitation.
Please see [the tutorial on installing packages](https://packaging.python.org/tutorials/installing-packages/) for more details.

## Storage Setup
Before you can use the `ddd` command line tool you must create a File System (container) at https://azurestorage.duhs.duke.edu/. 

### Config file setup.

DukeDSClient requires a config file at `~/.ddsclient` containing settings used to access the backing storge.
Minimally the config file must contain two fields:
- `azure_storage_account` - Azure storage account that contains your Azure container
- `azure_container_name` - Azure container where your projects(top level folders) will exist.

The simplest way to find these two values is from the **URL** field for your File System (container) at https://azurestorage.duhs.duke.edu/.

For example if the **URL** field is `https://mylab.dfs.core.windows.net/sequencing-data` the `azure_storage_account` field should be `mylab` and the `azure_container_name` should be `sequencing-data`.

The config file is in YAML format so for the above example the contents should be:
```
azure_storage_account: mylab
azure_container_name: sequencing-data
```
#### Delivery Config

If you wish to use the **deliver** command you must add a `delivery_token` field to the config file.
Email <gcb-help@duke.edu> for help getting this token.

### Usage:
See general help screen:

```
ddd -h
```

See help screen for a particular command:

```
ddd <command> -h
```

All commands take the form:
```
ddd <command> <arguments...>
```

### Upload:

```
ddd upload -p <ProjectName> <Folders/Files...>
```

This will create a project with the name ProjectName in the duke data service for your user if one doesn't exist.
It will then upload the Folders and it's contents to that project.
Any items that already exist with the same hash will not be uploaded.


Example: Upload a folder named 'results' to new or existing project named 'Analyzed Mouse RNA':

```
ddd upload -p 'Analyzed Mouse RNA' results
```

### Download:

```
ddd download -p <ProjectName> [Folder]
```

This will download the contents of ProjectName into the specified folder.
Currently it requires the directory be empty or not exist.
It will create Folder if it doesn't exist.
If Folder is not specified it will use the name of the project with spaces translated to '_'.

Example: Download the contents of project named 'Mouse RNA' into '/tmp/mouserna' :

```
ddd download -p 'Mouse RNA' /tmp/mouserna
```

#### Downloading to a file share

To download a project onto a file share (such as a CIFS share) specify a path within the share for the `Folder` to download directly there.


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
