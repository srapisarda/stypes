#!/bin/bash

DLP=$1
JAR_PATH="/Users/salvo.rapisarda/development/uni/stypes/target/scala-2.12/stypes-assembly-1.1.1.jar"
JAVA_CLASS="uk.ac.bbk.dcs.stypes.utils.NdlUtils"
JOIN_AND_UNION_BIN="/Users/salvo.rapisarda/development/uni/stypes/scripts/rust/number_of_join_and_union/target/release/number_of_join_and_union"

echo "filename,idbSet,edbSet,numCloses,numIdb,numEdb,avgClauseBodyLength,avgNumIDB,avgNumEDB,unions,joins,clauses"

INFO=$(java -cp "$JAR_PATH" "$JAVA_CLASS" "$DLP" info)
BASE_NAME_DLP=$(basename "$DLP")
JOIN_AND_UNION=$("$JOIN_AND_UNION_BIN" "$DLP")

echo "$BASE_NAME_DLP","$INFO","$JOIN_AND_UNION"
