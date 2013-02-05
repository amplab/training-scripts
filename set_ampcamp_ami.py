#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto
from optparse import OptionParser
from boto import *

def main():
  parser = OptionParser(usage="set_ampcamp_ami.py <ami-id>",
      add_help_option=True)
  (opts, args) = parser.parse_args()
  if len(args) != 1:
    parser.print_help()
    sys.exit(1)
  else:
    set_s3_kv("ampcamp-amis", "latest-strata", args[0])

def set_s3_kv(bucket, key, value):
  conn = connect_s3()
  s3_bucket = conn.get_bucket(bucket) 
  s3_key = s3_bucket.get_key(key)
  old_value = s3_key.get_contents_as_string()
  s3_key.set_contents_from_string(value)
  new_value = s3_key.get_contents_as_string()
  print "Changed value of " + key + " from " + old_value + " to " + new_value

if __name__ == "__main__":
  main()
