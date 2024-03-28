#!/bin/bash

while getopts f:p: option
do
case "${option}"
in
f) FILE_NAME=${OPTARG};;
p) FILE_PATH=${OPTARG};;
esac
done

if [ ! $FILE_NAME ] || [ ! $FILE_PATH ]; then
    echo "./command_mac.sh -p <FILE_PATH> -f <FILE_NAME>"
    echo "example: ./command_mac.sh -p ./files -f q11"
    exit 1
else
    PATH_LEN=${#FILE_PATH}
    if [ ${FILE_PATH: -1} == '/' ] && [ $PATH_LEN -gt 1  ] ; then
        FILE_PATH=${FILE_PATH::$PATH_LEN-1}
    fi


    FILE_TXT=$FILE_PATH"/"$FILE_NAME".cq"
    FILE_HG=$FILE_PATH"/"$FILE_NAME".hg"
    FILE_HTD=$FILE_PATH"/"$FILE_NAME".htd"
    FILE_GML=$FILE_PATH"/"$FILE_NAME".gml"

    echo "creating $FILE_HG from $FILE_TXT ..."
    python3 create_hg.py < $FILE_TXT > $FILE_HG

    echo "creating $FILE_HTD from $FILE_HG ..."
    ./htd_main-1.2.0 --input lp < $FILE_HG --output human > $FILE_HTD

    echo "creating $FILE_GML from $FILE_HTD ..."
    python3 transform-td.py $FILE_HTD > $FILE_GML

    echo "all done."

fi

exit 0