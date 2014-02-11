#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import subprocess
import sys
import time

from optparse import OptionParser
from sys import stderr

def parse_args():
  parser = OptionParser(usage="launch_ampcamp4_cluster [options] <path_to spark-ec2>",
      add_help_option=True)

  parser.add_option("--start-clusters", type="int", default=1,
      help="Start index from which cluster names are generated (useful for debugging)")
  parser.add_option("-n", "--clusters", type="int", default=1,
      help="Number of clusters to launch (default: 1)")

  parser.add_option("-p", "--parallel", type="int", default=1,
      help="Number of launches that will happen in parallel (default: 1)")

  # spark-ec2 options that are just passed through
  parser.add_option("-s", "--slaves", type="int", default=5,
      help="Number of slaves to launch (default: 5)")
  parser.add_option("-k", "--key-pair",
      help="Key pair to use on instances")
  parser.add_option("-i", "--identity-file",
      help="SSH private key file to use for logging into instances")
  parser.add_option("-a", "--ami", default="latest",
      help="Amazon Machine Image ID to use, or 'latest' to use latest " +
           "availabe ampcamp4 AMI")
  parser.add_option("-t", "--instance-type", default="m1.xlarge",
      help="Type of instance to launch (default: m1.xlarge). " +
           "WARNING: must be 64-bit; small instances won't work")
  parser.add_option("-r", "--region", default="us-east-1",
      help="EC2 region zone to launch instances in")

  parser.add_option("-w", "--wait", type="int", default=120,
      help="Seconds to wait for nodes to start (default: 120)")
  parser.add_option("--stagger", type="int", default=2,
      help="Seconds to stagger parallel launches (default: 2 seconds)")

  parser.add_option("--action", default="launch",
      help="Action to be used while calling spark-ec2 (default: launch)")
  parser.add_option("--copy", action="store_true", default=False,
      help="Copy AMP Camp data to ephemeral HDFS after launching the cluster (default: false)")

  (opts, args) = parser.parse_args()
  if len(args) != 1:
    parser.print_help()
    sys.exit(1)

  return (opts, args[0])

def main():
  (opts, spark_script_path) = parse_args()
  subprocesses = []
  cluster_names = []

  for cluster in range(opts.start_clusters, opts.start_clusters + opts.clusters):
    # Sleep for `stagger` seconds
    time.sleep(opts.stagger)

    # Launch a cluster
    args = []
    args.append(spark_script_path)

    args.append('-a')
    args.append(opts.ami)
    args.append('-k')
    args.append(opts.key_pair)
    args.append('-i')
    args.append(opts.identity_file)
    args.append('-s')
    args.append(str(opts.slaves))
    args.append('-t')
    args.append(opts.instance_type)
    args.append('-w')
    args.append(str(opts.wait))

    args.append('-r')
    args.append(opts.region)

    if opts.copy:
      args.append('--copy')

    args.append(opts.action)

    cluster_name = 'ampcamp4-' + str(cluster)
    args.append(cluster_name)

    print "Launching " + cluster_name
    print  args
    proc = subprocess.Popen(args, stdout=open("/tmp/" + cluster_name + "-" + opts.action + ".out", "w"),
                            stderr=open("/tmp/" + cluster_name + "-" + opts.action + ".err", "w"))
    subprocesses.append(proc)
    cluster_names.append(cluster_name)

    # Wait for all the parallel launches to finish
    if (len(subprocesses) == opts.parallel):
      ret = wait_and_check(subprocesses, cluster_names, opts)
      subprocesses = []
      cluster_names = []
      if ret != 0:
        print >> stderr, ("ERROR: Wait and check failed. Exiting")
        sys.exit(-1)

  if (len(subprocesses) != 0):
    wait_and_check(subprocesses, cluster_names, opts)

def wait_and_check(subprocesses, cluster_names, opts):
  num_success = 0
  num_spark_failed = 0
  print "Waiting for parallel launches to finish...."
  # Print out details about clusters we launched 
  for p in range(0, len(subprocesses)):
    subprocesses[p].wait()
    p_stderr = open("/tmp/" + cluster_names[p] + "-" + opts.action + ".err")
    p_stdout = open("/tmp/" + cluster_names[p] + "-" + opts.action + ".out")
    errs = p_stderr.readlines()
    for err in errs:
      if "SUCCESS:" in err:
        num_success = num_success + 1
        parts = err.split()
        master_name = parts[len(parts) - 1]
        master_name = master_name.replace('\r', '' )
        print "INFO: Cluster " + cluster_names[p] + " " + master_name.strip() + "\n"
        break
      elif "ERROR: spark-check" in err:
        num_spark_failed = num_spark_failed + 1
        break

  if (num_success != len(subprocesses)):
    print "ERROR: Failed to launch all clusters " + str(num_spark_failed) + " failed spark check"
    return -1
  else:
    return 0

if __name__ == "__main__":
  logging.basicConfig()
  main()
