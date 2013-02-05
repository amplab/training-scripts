#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import subprocess
import sys

from optparse import OptionParser
from sys import stderr

def parse_args():
  parser = OptionParser(usage="launch_strata_cluster [options] <path_to spark-ec2>",
      add_help_option=True)

  parser.add_option("--start-clusters", type="int", default=1,
      help="Start index from which cluster names are generated (useful for debugging)")
  parser.add_option("-n", "--clusters", type="int", default=1,
      help="Number of clusters to launch (default: 1)")
  parser.add_option("-b", "--buckets", type="int", default=1,
      help="number of s3 bucket clones (default: 1)")
  parser.add_option("--s3-stats-bucket", default="ampcamp-data/wikistats_20090505-0",
      help="base-name of S3 bucket to copy strata data from. The bucket number " +
           " will be appended at the end to form the complete s3 key " +
           " (default: ampcamp-data/wikistats_20090505-0)")

  parser.add_option("--s3-small-bucket", default="ampcamp-data/wikistats_20090505_restricted-0",
      help="base-name of S3 bucket to copy restricted strata data from. The bucket number " +
           " will be appended at the end to form the complete s3 key " +
           " (default: ampcamp-data/wikistats_20090505_restricted-0)")

  parser.add_option("-p", "--parallel", type="int", default=1,
      help="Number of launches that will happen in parallel (default: 1)")

  # spark-ec2 options that are just passed through
  parser.add_option("-s", "--slaves", type="int", default=3,
      help="Number of slaves to launch (default: 3)")
  parser.add_option("-k", "--key-pair",
      help="Key pair to use on instances")
  parser.add_option("-i", "--identity-file", 
      help="SSH private key file to use for logging into instances")
  parser.add_option("-a", "--ami", default="latest",
      help="Amazon Machine Image ID to use, or 'latest' to use latest " +
           "availabe spark AMI, 'standalone' for the latest available " +
           "standalone AMI (default: latest)")

  parser.add_option("--action", default="launch",
      help="Action to be used while calling spark-ec2 (default: launch)") 
  parser.add_option("--copy", action="store_true", default=False,
      help="Copy AMP Camp data from S3 to ephemeral HDFS after launching the cluster (default: false)")

  (opts, args) = parser.parse_args()
  if len(args) != 1:
    parser.print_help()
    sys.exit(1)

  return (opts, args[0])

def main():
  (opts, spark_script_path) = parse_args()
  s3_buckets = range(1, opts.buckets + 1)
  availability_zones = ["us-east-1b", "us-east-1d", "us-east-1a"]
  subprocesses = []
  cluster_names = []

  for cluster in range(opts.start_clusters, opts.start_clusters + opts.clusters):
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
    
    args.append('-z')
    args.append(availability_zones[cluster % len(availability_zones)])
    
    args.append('--s3-stats-bucket')
    args.append(opts.s3_stats_bucket + str(s3_buckets[cluster%len(s3_buckets)]))
    args.append('--s3-small-bucket')
    args.append(opts.s3_small_bucket + str(s3_buckets[cluster%len(s3_buckets)]))
    
    if opts.copy:
      args.append('--copy')

    args.append(opts.action)

    cluster_name = 'strata' + str(cluster)
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
