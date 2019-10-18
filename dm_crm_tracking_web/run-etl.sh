#!/bin/bash

CURRENT_PATH=${BASH_SOURCE%/*}

exec_date=${1}
env=${2}
end_date=${3}

spark-submit --conf spark.dynamicAllocation.enabled=false \
             --executor-cores 4 \
             --num-executors 4 \
             ${CURRENT_PATH}/etl.py ${exec_date} ${env} ${end_date}
err=${?}

if [[ ${err} -gt 0 ]]; then
    exit ${err}
fi
