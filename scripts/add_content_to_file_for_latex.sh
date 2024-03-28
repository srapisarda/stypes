#!/bin/bash

while getopts p:q: option
do
case "${option}"
in
p) FILE_PATH=${OPTARG};;
q) QUERY_NAME=${OPTARG};;
esac
done

if [ ! $FILE_PATH ] || [ ! $QUERY_NAME ]; then
    echo "example of use: ./add_content_to_file_for_latex.sh -p <FILE_PATH> -q <query_name>"
    exit 1
else
  q_name=$( sed 's/\///g' <<< $QUERY_NAME)
  DLP_FILES=$(find $FILE_PATH -name "*.dlp")
  for DLP in $DLP_FILES
  do
    echo '%'
    ndl_name=$(basename $DLP|  sed 's/rew_flatten_//g' |  sed 's/.dlp//g' )
    GIT_PATH=$(awk -F$QUERY_NAME '{print $2}' <<< $DLP )
    echo "\subsubsection{\href{https://github.com/srapisarda/stypes/blob/master/src/test/resources/rewriting/flatten/$q_name/$GIT_PATH}{$ndl_name}}"
    echo "\label{app:$ndl_name:ndl}"
    echo '\begin{lstlisting}[basicstyle=\tiny,language=prolog]'
    cat $DLP
    echo '\end{lstlisting}'
    echo '%'
  done

fi