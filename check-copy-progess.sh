#!/bin/bash
# Usage
# check-copy-progress <private_key_path> <file-with-hostnames>

pssh -i -x "-i $1" -l root -h $2 '~/ephemeral-hdfs/bin/hadoop fs -dus /'
