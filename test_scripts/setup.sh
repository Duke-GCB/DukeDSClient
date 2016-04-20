#!/usr/bin/env bash
if [ ! -e duke-data-service ]
then
    git clone https://github.com/Duke-Translational-Bioinformatics/duke-data-service.git
fi

docker build -f TestBaseDockerfile -t dds_test_base .
