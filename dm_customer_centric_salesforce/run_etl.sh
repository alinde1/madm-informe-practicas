#!/bin/bash

CURRENT_PATH=${BASH_SOURCE%/*}

exec_date=${1}
env=${2}

spark-submit --conf spark.dynamicAllocation.enabled=false \
             --num-executors 6 \
             --executor-cores 6 \
             --deploy-mode client \
             --driver-memory 5G \
             --master yarn \
             --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3 \
             --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=python3 \
             "${CURRENT_PATH}"/etl.py "${exec_date}" "${env}"
