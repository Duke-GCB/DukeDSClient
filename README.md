# DukeDSClient
Command line tool to upload/manage project on the [duke-data-service](https://github.com/Duke-Translational-Bioinformatics/duke-data-service).
[![Build Status](https://travis-ci.org/Duke-GCB/DukeDSClient.svg?branch=master)](https://travis-ci.org/Duke-GCB/DukeDSClient)

Runs on Python 2.7 or 3.5.

# Install:
```
pip install DukeDSClient
```

# Additional Setup:
This requires a [Duke NetID](https://oit.duke.edu/email-accounts/netid/).
To use DukeDSClient, it requires an agent key and a user key to be added to a config file.

## Agent Key
Hopefully your server will already have the global configuration setup.
If not you can temporarily use the Agent key you create below.

## Steps To Create a User Key
This key can be created at the [Duke Data Service Portal](https://dataservice.duke.edu).
####  1. Click login

####  2. Click top left menu button
![Alt text](/images/TopLeftMenuButton.png?raw=true "Top Left Menu Button")

####  3. Click 'Software Agents'
![Alt text](/images/TopLeftMenu.png?raw=true "Top Left Menu")

####  4. Click 'ADD NEW AGENT' button
![Alt text](/images/AddAgentButton.png?raw=true "Add Agent Button")

####  5. Fill in data for a new agent and click 'SUBMIT'.
![Alt text](/images/CreateAgent.png?raw=true "Create Agent")

####  6. Click Agent you just created.
![Alt text](/images/ClickAgent.png?raw=true "Click Agent")

####  7. Click Key Menu button.
![Alt text](/images/KeyMenuButton.png?raw=true "Key Menu Button")

####  8. Select 'User Secret Key'.
![Alt text](/images/KeyMenu.png?raw=true "User Secret Key")

####  9. Click Create New Key.
![Alt text](/images/CreateNewKey.png?raw=true "Create New Key")

####  10. Click 'COPY KEY TO CLIPBOARD'.
![Alt text](/images/CopyKeyToClipboard.png?raw=true "Copy Key to Clipboard")


You how have your user key in your clipboard.

#### Add Keys to Config file
Create a file in the ~/.ddsclient location and add your user key value.
```
user_key: <USER_KEY>
```
If your global configuration isn't already setup you can manually add the Agent Secret Key you created above:
```
user_key: <USER_KEY>
agent_key: <AGENT_KEY>
```

###Use:
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

###Upload:
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


###Add User To Project:
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


###Testing:
From the root directory run the following:
```
python setup.py test
```

### Data Service Web Portal:
[Duke Data Service Portal](https://dataservice.duke.edu).
This also requires a [Duke NetID](https://oit.duke.edu/email-accounts/netid/).

### Advanced:
You can specify an alternate url for use with ddsclient via the `DUKE_DATA_SERVICE_URL` environment variable.
Here is how you can set the environment variable so ddsclient will connect to the 'dev' url:
```
export DUKE_DATA_SERVICE_URL='https://apidev.dataservice.duke.edu/api/v1'
```
This will require using the associated apiexplorer to get a valid token.

