#!/bin/bash
# Usage
# masters.sh <private_key_path> <file-with-hostnames> <commands_to_run>

if [ $# -le 2 ];then
  echo "Usage masters.sh <private_key_path> <file-with-hostnames> <commands_to_run>"
  exit 1
fi

KEY_FILE=$1
MASTERS_FILE=$2

shift
shift

pssh -i -x "-i $KEY_FILE" -l root -h $MASTERS_FILE "$@"
