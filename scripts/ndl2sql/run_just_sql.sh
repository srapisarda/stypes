#!/bin/bash

SQL_FILE=$1
STYPES_FLINK_JAR=/home/hduser/development/stypes-flink/target/scala-2.12/stypes-flink_2.12-1.0.jar
OPTIMISE=$2

for PAR in 5 10 15 20
do

for TTL in 3 5 7 9
do

SQL=$(cat "$SQL_FILE")
echo ${SQL}


/opt/flink/bin/flink run \
  -c uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSql \
  -p $PAR \
   $STYPES_FLINK_JAR $TTL "${SQL_FILE}_par-${PAR}_ttl-${TTL}" $OPTIMISE  "$SQL"

#echo "submitte q45  ttl: $ttl, par: $par"
#sleep 30

done

done

