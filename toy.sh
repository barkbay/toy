#!/bin/bash

if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi


if [ "$HADOOP_CONF_DIR" = "" ]; then
  echo "Error: HADOOP_CONF_DIR is not set."
  exit 1
fi

if [ "$HADOOP_YARN_HOME" = "" ]; then
  echo "Error: HADOOP_YARN_HOME is not set."
  exit 1
fi

yarn=${HADOOP_YARN_HOME}/bin/yarn

if [[ -x "$yarn" ]]
then
    echo "yarn is @ $yarn"
else
    echo "File $yarn is not executable or found"
    exit 1
fi

here=`dirname "${BASH_SOURCE-$0}"`
cd $here
echo "CWD=$PWD"

export HADOOP_CLASSPATH=./*:$here/lib-ext/*
echo "Client classpath is ${HADOOP_CLASSPATH}"

toyjar=`ls $here/toy-*.jar`

exec "$yarn" jar "$toyjar" "$@"
