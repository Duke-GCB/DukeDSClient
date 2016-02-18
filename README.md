# DukeDSClient
Command line tool to upload the contents of a folder into a project on the [duke-data-service](https://github.com/Duke-Translational-Bioinformatics/duke-data-service).

###Install:
```
git clone https://github.com/Duke-GCB/DukeDSClient.git
cd DukeDSClient
python setup.py install
```

###Use:
See help screen:
```
ddsclient -h
```

Basic usage takes the form:
```
ddsclient <projectname> <folder>
```
Where projectname is the name of the project to add folder too.

Example: Upload a folder named 'results' to new or existing project named 'Analyzed Mouse RNA':
```
ddsclient 'Analyzed Mouse RNA' results
```
