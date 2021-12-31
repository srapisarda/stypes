#!/bin/bash

function join_by { local d=${1-} f=${2-}; if shift 2; then printf %s "$f" "${@/#/$d}"; fi; }

WORKING_DIR=$1
LOG_FILE="${WORKING_DIR}/$2"
JOBS=$(more "$LOG_FILE.log"  | grep "submitted with JobID" | cut -d ' ' -f7)
#echo $JOBS

JOBS_ARGS=$(join_by ' ' $JOBS )
for j in $JOBS
do
  hadoop fs -get /flink/completed-jobs/$j "${WORKING_DIR}/."
done

CSV_STATS_FILE="${WORKING_DIR}/${LOG_FILE}"
python3 get_job_statistic.py -o "$CSV_STATS_FILE.csv" -j $JOBS_ARGS



