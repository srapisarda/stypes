#!/bin/bash

while getopts p: option
do
case "${option}"
in
p) FILE_PATH=${OPTARG};;
esac
done

if [ ! $FILE_PATH ]; then
    echo "example of use: ./idb_def_count_head_body.sh -p <FILE_PATH>"
    exit 1
else
    PATH_LEN=${#FILE_PATH}
    if [ ${FILE_PATH: -1} == '/' ] && [ $PATH_LEN -gt 1  ] ; then
        FILE_PATH=${FILE_PATH::$PATH_LEN-1}
    fi

    files=$(find ../. -name "*-rew_flatten_*.dlp") #Add () to convert output to array
    echo "rewriting,idb,head,body"
    for file in $files ; do
      java -cp ../target/scala-2.12/stypes-assembly-1.1.1.jar uk.ac.bbk.dcs.stypes.utils.NdlUtils $file idb-def-count
    done
fi

exit 0