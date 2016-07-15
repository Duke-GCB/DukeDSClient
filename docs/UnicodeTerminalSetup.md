##  Unicode Terminal Setup

DukeDSClient does not support ASCII terminal encoding. 
UTF-8 is our preferred encoding.
Adjust your terminal settings so you will be able to see unicode characters.

__OSX Terminal Settings__
Open the Terminal application and chose Preferences menu item.
![OSX Terminal Setting](images/OSXTerminalMenu.png?raw=true "OSX Terminal Preferences")

Make sure your Text encoding is Unicode.
![OSX Terminal Setting](images/OSXTerminalSetting.png?raw=true "OSX Terminal Setting")
This screen appears to save whenever the value is changed.
However you may need to start a new terminal for the new settings to take effect.

__Command Line Envionment Variables__
_This should only be necessary if you are unable to adjust your terminal settings at the operation system level._
 
If you run the following on Linux or OSX:
```
export LANG=en_US.UTF-8
```
it will temporary change your terminal encoding to UTF-8.

