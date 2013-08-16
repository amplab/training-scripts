#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import json
import urllib2
import boto
from optparse import OptionParser
from boto import *

def is_active(instance):
  return (instance.state in ['pending', 'running', 'stopping', 'stopped'])

def main():
  parser = OptionParser(usage="get_masters [cluster_prefix]",
      add_help_option=True)
  (opts, args) = parser.parse_args()
  if len(args) != 1:
    get_cluster_masters()
  else:
    get_cluster_masters(args[0])

def get_cluster_masters(prefix=""):
  conn = connect_ec2()
  res = conn.get_all_instances()
  # master reservations have only one node
  masters = [i for i in res if len(i.instances) == 1]
  master_instances = [m.instances[0] for m in masters if is_active(m.instances[0])]

  name_host = [(m.tags['cluster'], m.public_dns_name) for m in master_instances if 'cluster' in m.tags]

  for (name, host) in name_host:
    if prefix in name:
      print(name + " " + host)

if __name__ == "__main__":
  main()
