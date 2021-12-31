#!/bin/bash

function join_by { local d=${1-} f=${2-}; if shift 2; then printf %s "$f" "${@/#/$d}"; fi; }

WORKING_DIR=$1
LOG_FILE="${WORKING_DIR}/$2"
JOBS=$(more "$LOG_FILE.log"  | grep "submitted with JobID" | cut -d ' ' -f7)
#echo $JOBS

JOBS_DIR="${WORKING_DIR}/jobs"
rm -rf "$JOBS_DIR"
mkdir "$JOBS_DIR"

for j in $JOBS
do
  hadoop fs -get /flink/completed-jobs/$j "${JOBS_DIR}/."
done

JOBS_ARGS=$(join_by " ${JOBS_DIR}/" $JOBS )
echo "$JOBS_ARGS"
CSV_STATS_FILE="${LOG_FILE}.csv"
python3 get_job_statistic.py -o "$CSV_STATS_FILE" -j "${JOBS_DIR}/$JOBS_ARGS"



