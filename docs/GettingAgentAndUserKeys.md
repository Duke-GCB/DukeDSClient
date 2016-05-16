### Getting an Agent Key and a User Key:
DukeDSClient requires a config file containing an __agent_key__ and a __user_key__.
This requires a [Duke NetID](https://oit.duke.edu/email-accounts/netid/).

## Steps To Create your Agent and User Keys
Go to the [Duke Data Service Portal](https://dataservice.duke.edu).
####  1. Click login
####  2. Click top left menu button
![Top Left Menu Button](images/TopLeftMenuButton.png?raw=true "Top Left Menu Button")
####  3. Click 'Software Agents'
![Top Left Menu](images/TopLeftMenu.png?raw=true "Top Left Menu")
####  4. Click 'ADD NEW AGENT' button
![Add Agent Button](images/AddAgentButton.png?raw=true "Add Agent Button")
####  5. Fill in data for a new agent and click 'SUBMIT'.
![Create Agent](images/NewSoftwareAgent.png?raw=true "Create Agent")

You can use 'DukeDSClient" followed by your name for the Name and Description fields.
####  6. Click 'CREDENTIALS' you just created.
![Click Agent](images/ClickCredentials.png?raw=true "Click CREDENTIALS")
####  7. Click 'COPY CREDENTIALS'.
![Key Menu Button](images/CopyCredentials.png?raw=true "Key Menu Button")

You how have your user and agent key in your clipboard.

Create a file at ~/.ddsclient and paste your clipboard contents into it.
It should look similar to this:
```
{
    "agent_key": "<AGENT_KEY>",
    "user_key": "<USER_KEY>"
}
```

You should be able to now use ddsclient: [README.md](../README.md)


## Custom url configuration
If you are working with the uatest or dev server you will need to add the appropriate url to ~/.ddsclient.

```
{
    "url": "https://apidev.dataservice.duke.edu/api/v1",
    "agent_key": "<AGENT_KEY>",
    "user_key": "<USER_KEY>"
}
```            
