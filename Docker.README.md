# Running duke-data-service in docker

### Download and configure duke-data-service
On Mac you need to be running inside docker terminal for this:
```
git clone https://github.com/Duke-Translational-Bioinformatics/duke-data-service.git
cd duke-data-service
git checkout <some_branch>
rm swift.env
echo 'NOFORCESSL=true' >> webapp.env
ln -s swift.local.env swift.env
```

### Start the local duke-data-service
```
./launch_application.sh
```
You should now have three images running in docker: dukedataservice_swift, dukedataservice_server, dukedataservice_db.
```
docker ps
```
Note the tag for dukedataservice_server typically(dukedataservice_server_1)

### Create auth token
```
docker exec -it dukedataservice_server_1 sh -c 'token=`rake api_test_user:create`; echo $token'
```
This will print out a bunch of debug stuff but the last 'word' will be be the auth token.
They seem to always start with 'ey' for some reason.
Put auth into your ~/.ddsclient config file.
```
auth: 'eyJ0eXAiOiJKV...jmqXCQ99wwo'
```

### Setup host address
Determine what IP address to talk to.
```
docker-machine ip
```
Put this url into your ~/.ddsclient config file.
```
url: 'http://<IP>:3001/api/v1'
```

### Test it
```
ddsclient upload -p TestProject ...
```
