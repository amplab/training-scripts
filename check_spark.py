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
  parser = OptionParser(usage="check_spark [spark_master_hostname]",
      add_help_option=True)
  (opts, args) = parser.parse_args()
  if len(args) != 1:
    check_all_masters()
  else:
    check_spark_master(args[0])

def check_all_masters():
  conn = connect_ec2()
  res = conn.get_all_instances()
  # master reservations have only one node
  masters = [i for i in res if len(i.instances) == 1]
  master_instances = [m.instances[0] for m in masters if is_active(m.instances[0])]

  name_host = [(m.tags['cluster'], m.public_dns_name) for m in master_instances]
  
  for (name, host) in name_host:
    print(name + " " + host, end=' ')
    try:
      check_spark_master(host)
    except Exception:
      print("Spark master DOWN ")

def check_spark_master(master_hostname):
  #url = "http://" + master + ":8080"
  master_url = "http://" + master_hostname + ":8080"
  url = master_url + "/json"
  response = urllib2.urlopen(url, timeout=30)
  if response.code != 200:
    print("Spark master " + url + " returned " + str(response.code))
    return -1
  master_json = response.read()
  return check_spark_json(master_json)

def check_spark_json(spark_json):
  json_data = json.loads(spark_json)
  ## Find number of cpus from status page
  got_num_cpus = int(json_data.get("cores"))
  print("Spark master reports " + str(got_num_cpus) + " CPUs")

if __name__ == "__main__":
  main()
