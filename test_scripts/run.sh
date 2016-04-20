#!/usr/bin/env bash

build_docker_file() 
{
  echo "Building docker file"
  # rebuild with our sources
  docker build -f TestDockerfile -t dds_test . >/dev/null
}

start_dds()
{
  echo "Setup duke-data-service"
  cd duke-data-service
  rm swift.env 2>/dev/null
  echo 'NOFORCESSL=true' >> webapp.env
  ln -s swift.local.env swift.env
  ./launch_application.sh >/dev/null 2>/dev/null
  cd ..

  trap "cleanup_dds; exit" EXIT
}

cleanup_dds()
{
  echo "Cleanup duke-data-service"
  cd duke-data-service
  docker-compose down 2>/dev/null >/dev/null
}

create_user_data()
{
  echo "Create user and keys in duke-data-service"
  # create a user
  docker exec -it dukedataservice_server_1 sh -c 'rake api_test_user:create' >/dev/null

  # get user key
  DDS_USER_KEY=`docker exec -it dukedataservice_server_1 sh -c 'rake api_test_user:api_key | tail -n 1'`

  # get agent key
  DDS_AGENT_KEY=`docker exec -it dukedataservice_server_1 sh -c 'rake api_test_user:software_agent_api_key | tail -n 1'`

  # get IP for data service
  DDS_IP=`docker-machine ip`
}

delete_user_data()
{
  echo "Delete user and data in duke-data-service"
  # delete a user
  docker exec -it dukedataservice_server_1 sh -c 'rake api_test_user:destroy' >/dev/null
}
run_test()
{
    TEST_DESC=$1
    TEST_PYTHON=$2
    UPLOAD_CHUNK_SIZE=$3
    UPLOAD_WORKERS=$4
    create_user_data
    #run python2 tests
    TEMPFILE=/tmp/DukeDSClientTest.$$
    echo "Running tests for $TEST_PYTHON uploadSize:$UPLOAD_CHUNK_SIZE uploadWorkers:$UPLOAD_WORKERS" | tee $TEMPFILE
    docker run -it -e UPLOAD_CHUNK_SIZE=$UPLOAD_CHUNK_SIZE -e UPLOAD_WORKERS=$UPLOAD_WORKERS \
                   -e TEST_PYTHON=$TEST_PYTHON -e DDS_IP=$DDS_IP \
                   -e DDS_USER_KEY=$DDS_USER_KEY -e DDS_AGENT_KEY=$DDS_AGENT_KEY \
                   -v /tmp/DukeDSClientData/hg38.chromFaMasked.tar.gz \
                   -v /tmp/DukeDSClientData/hg38.fa.gz \
                   dds_test >> $TEMPFILE
    if [ "$?" -ne "0" ]
    then
      echo "ERROR: $TEST_PYTHON tests failed"
      tail $TEMP
      echo "see $TEMP for more"
      exit 1
    fi
    delete_user_data
    rm $TEMPFILE
}

build_docker_file
start_dds

run_test python2 python 100MB 1
run_test python3 python3 100MB 1

echo "SUCCESS"
exit 0
