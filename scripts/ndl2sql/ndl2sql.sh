#!/bin/bash

NDL=$1
STYPES_JAR=stypes-assembly-1.1.1.jar

for PREDICATE in "p3"
do

for PAR in 10
do

for TTL in 5
do

NDL_FLATTEN_FILE="$1_rew.dlp"


# shellcheck disable=SC2091
NDL_FLATTEN=$(java -cp $STYPES_JAR  uk.ac.bbk.dcs.stypes.utils.NdlSubstitution "$NDL" $PREDICATE)

echo "$NDL_FLATTEN" > "$NDL_FLATTEN_FILE"

SQL=$(java -cp $STYPES_JAR uk.ac.bbk.dcs.stypes.sql.SqlUtils "$NDL_FLATTEN_FILE" "$2")
#echo ${SQL}

java -cp $STYPES_JAR  uk.ac.bbk.dcs.stypes.utils.UtilRunner  "$SQL" abc

/opt/flink/bin/flink run \
  -c uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSql \
  -p $PAR \
   /home/hduser/development/stypes-flink/target/scala-2.11/stypes-flink_2.11-1.0.jar $TTL sql_$PAR_$TTL true $SQL

#echo "submitte q45  ttl: $ttl, par: $par"
#sleep 30

done

done

done