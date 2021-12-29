#!/bin/bash

NDL=$1
STYPES_JAR=/home/hduser/development/stypes/target/scala-2.12/stypes-assembly-1.1.1.jar
STYPES_FLINK_JAR=/home/hduser/development/stypes-flink/target/scala-2.12/stypes-flink_2.12-1.0.jar
IFS="," read -ra PREDICATES <<< "$3";
echo "flatten for:  ${PREDICATES[@]}"

for PREDICATE in ${PREDICATES[@]}
do

for PAR in 10
do

for TTL in 5
do

NDL_FLATTEN_FILE="${NDL}_rew_${PREDICATE}.dlp"


# shellcheck disable=SC2091
NDL_FLATTEN=$(java -cp $STYPES_JAR  uk.ac.bbk.dcs.stypes.utils.NdlFlatten "$NDL" $PREDICATE)

echo "$NDL_FLATTEN" > "$NDL_FLATTEN_FILE"

SQL=$(java -cp $STYPES_JAR uk.ac.bbk.dcs.stypes.sql.SqlUtils "$NDL_FLATTEN_FILE" "$2")
echo ${SQL}

#java -cp $STYPES_JAR  uk.ac.bbk.dcs.stypes.utils.UtilRunner  "$SQL" abc

/opt/flink/bin/flink run \
  -c uk.ac.bbk.dcs.stypes.flink.FlinkRewritingSql \
  -p $PAR \
   $STYPES_FLINK_JAR $TTL "${NDL}_par-${PAR}_ttl-${TTL}_p-${PREDICATE}" true "$SQL"

#echo "submitte q45  ttl: $ttl, par: $par"
#sleep 30

done

done

done