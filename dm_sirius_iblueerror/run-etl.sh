#!/bin/bash

CURRENT_PATH=${BASH_SOURCE%/*}

env=${1}     # devel | live


function message {
    echo
    echo "[$(date +'%Y-%m-%d %H:%M')] ${1}"
    echo
}

function check_for_errors {
    err=${?}
    if [[ ${err} -gt 0 ]]; then
        exit ${err}
    fi
}


message "Creating virtual environment"
VENV=/tmp/dm-env3
python3.6 -m venv ${VENV}
# shellcheck source=src/bin/activate
. ${VENV}/bin/activate

message "Installing requirements"
PYTHON=${VENV}/bin/python3
JARS=/tmp
pip -q install JPype1==0.6.3
pip -q install jaydebeapi
pip -q install pyspark
aws s3 cp s3://bucket-cdr-main-"${env}"/CRM/jars/ojdbc8.jar ${JARS}


message "Create Hive table"
hive --silent \
     --hiveconf hive.execution.engine=tez \
     --hiveconf hive.exec.dynamic.partition.mode=nonstrict \
     --hivevar env="${env}" \
     -f "${CURRENT_PATH}"/iblueerror.ql
check_for_errors


message "Spark-Submit"
spark-submit --conf spark.dynamicAllocation.enabled=false \
             --master local \
             --executor-cores 1 \
             --num-executors 1 \
             --executor-memory 8G \
             --jars ${JARS}/ojdbc8.jar \
             --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=${PYTHON} \
             --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=${PYTHON} \
             "${CURRENT_PATH}"/etl.py "${env}"
check_for_errors

message "Remove temporal files"
deactivate
rm -rf ${VENV}
rm ${JARS}/ojdbc8.jar

message "Done"
