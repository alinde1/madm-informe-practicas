#!/bin/bash

CURRENT_PATH=${BASH_SOURCE%/*}
DM_UTILS_PATH=${CURRENT_PATH}/../../utils

exec_date=${1} # YYYY-MM-DD_HH
env=${2}       # devel | live

year=`echo ${exec_date}  | cut -d- -f1`
month=`echo ${exec_date}  | cut -d- -f2`
day=`echo ${exec_date}  | cut -d- -f3 | cut -d_ -f1`
hour=`echo ${exec_date}  | cut -d_ -f2`

S3_DIR=`aws s3 ls s3://bucket-cdr-landing-${env}/AVAYA/${year}/${month}/${day}/${hour}`
if [[ $? != 0 ]]; then
  echo ${S3_DIR} does not exists
  exit 0
fi

function check_for_errors {
    err=$?
    if [[ ${err} -gt 0 ]]; then
        exit ${err}
    fi
}

function message {
    echo
    echo "[${year}/${month}/${day}_${hour}] ${1}"
    echo
}

# change to spark-sql
function execute_script {
    hive --silent \
         --hiveconf hive.execution.engine=tez \
         --hiveconf hive.exec.dynamic.partition.mode=nonstrict \
         --hivevar env=${env} \
         --hivevar year=${year} \
         --hivevar month=${month} \
         --hivevar day=${day} \
         --hivevar hour=${hour} \
         --hivevar current_path=${CURRENT_PATH} \
         -f ${CURRENT_PATH}/${1}
}

message "Executing HiveQL script..."
execute_script "etl.ql"
check_for_errors
