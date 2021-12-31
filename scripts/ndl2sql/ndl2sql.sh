#!/bin/bash

WORKING_DIR=$1
NDL_FILE="${WORKING_DIR}/${2}.dlp"
EDB_CATALOG="${WORKING_DIR}/${3}.dlp"
STYPES_JAR=/home/hduser/development/stypes/target/scala-2.12/stypes-assembly-1.1.1.jar

echo "$NDL_FILE to SQL"

SQL=$(java -cp $STYPES_JAR uk.ac.bbk.dcs.stypes.sql.SqlUtils "$NDL_FILE" "$EDB_CATALOG")
echo ${SQL}

SQL_FILE="${NDL_FILE}.sql"
echo "$SQL" > "$SQL_FILE"

echo SQL file "$NDL_FILE" has been written