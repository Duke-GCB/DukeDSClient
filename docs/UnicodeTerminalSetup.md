##  Unicode Terminal Setup

DukeDSClient does not support ASCII terminal encoding. 
UTF-8 is our preferred encoding.
This allows you to see unicode characters in project names, folders and filenames.

### OSX Terminal Settings
Open the Terminal application and chose Preferences menu item.

![OSX Terminal Setting](images/OSXTerminalMenu.png?raw=true "OSX Terminal Preferences")

Make sure your Text encoding is Unicode.
![OSX Terminal Setting](images/OSXTerminalSetting.png?raw=true "OSX Terminal Setting")

This screen saves whenever the value is changed, however you will need to start a new terminal for the new settings to take effect.

### Command Line Envionment Variables
_This is only necessary if you are unable to adjust your terminal settings at the operation system level._
 
If you run the following on Linux or OSX:
```
export PYTHONIOENCODING=UTF-8
```
it will temporary change your python encoding to UTF-8.

