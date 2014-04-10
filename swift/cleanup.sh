#!/bin/bash

containers="$(swift list --prefix dev)"
#echo ${containers}
for container in ${containers}; do
    echo "${container}"
    swift delete --object-threads 128  --container-threads 4 ${container}
done
