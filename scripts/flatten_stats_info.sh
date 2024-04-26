#!/bin/bash

WORKING_DIR=$1
JAR_PATH="/Users/salvo.rapisarda/development/uni/stypes/target/scala-2.12/stypes-assembly-1.1.1.jar"
JAVA_CLASS="uk.ac.bbk.dcs.stypes.utils.NdlUtils"


FOLDERS=$(find "$WORKING_DIR"/p* -type d)

for FOLD in ${FOLDERS[@]}
do
#  echo "$FOLD"
  DLPS=$(ls "$FOLD"/*.dlp)
  for DLP in ${DLPS[@]}
  do
    INFO=$(java -cp "$JAR_PATH" "$JAVA_CLASS" "$DLP" info)
    BASE_NAME_DLP=$(basename "$DLP")

    echo "$BASE_NAME_DLP","$INFO"
  done
done