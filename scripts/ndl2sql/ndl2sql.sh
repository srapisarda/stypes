#!/bin/bash

NDL=$1
EDB_CATALOG=$2
STYPES_JAR=/home/hduser/development/stypes/target/scala-2.12/stypes-assembly-1.1.1.jar
echo "ndl to SQL for:  $NDL"

SQL=$(java -cp $STYPES_JAR uk.ac.bbk.dcs.stypes.sql.SqlUtils "$NDL" "$EDB_CATALOG")
echo ${SQL}