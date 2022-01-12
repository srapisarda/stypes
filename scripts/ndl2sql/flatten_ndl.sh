#!/bin/bash

if [ "$#" -ne 3 ]
then
  echo "Illegal number of parameters"
  echo "use: flatten_ndl_2_sql.sh <WORKING_DIR> <NDL_FILE without extension>  <p1>,<p2>,<p3>,...<pN>"
  exit 1
fi

WORKING_DIR=$1
NDL_FILE="${2}"
# STYPES_JAR=/home/hduser/development/stypes/target/scala-2.12/stypes-assembly-1.1.1.jar
STYPES_JAR=/Users/salvo.rapisarda/development/uni/stypes/target/scala-2.12/stypes-assembly-1.1.1.jar

IFS="," read -a PREDICATES <<< $3;

echo "flatten for: " "${PREDICATES[@]}"

for PREDICATE in ${PREDICATES[@]}
do

NDL_FLATTEN=$(java -cp $STYPES_JAR  uk.ac.bbk.dcs.stypes.utils.NdlFlatten "${WORKING_DIR}/${NDL_FILE}.dlp" "${PREDICATE}")

NDL_FLATTEN_FILE="${NDL_FILE}_flatten_${PREDICATE}"
echo "$NDL_FLATTEN" > "${WORKING_DIR}/$NDL_FLATTEN_FILE.dlp"

echo "flattened NDL by removing ${PREDICATE} into ${WORKING_DIR}/$NDL_FLATTEN_FILE.dlp"

done