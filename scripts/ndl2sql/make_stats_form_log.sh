#!/bin/bash

function join_by { local d=${1-} f=${2-}; if shift 2; then printf %s "$f" "${@/#/$d}"; fi; }

LOG_FILE="$1"
JOBS=$(more $LOG_FILE  | grep "submitted with JobID" | cut -d ' ' -f7)
#echo $JOBS

JOBS_ARGS=$(join_by ' ' $JOBS )
for j in $JOBS
do
  hadoop fs -get /flink/completed-jobs/$j .	
done

python3 get_job_statistic.py -o "$LOG_FILE.csv"  -j $JOBS_ARGS 



