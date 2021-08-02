#!/usr/bin/env python3
import configparser

from emrutils import *
from hbasemetrics import *
from masterhbasemetrics import *
from s3metrics import *

config = configparser.ConfigParser(allow_no_value=True, delimiters=('='))
config.read('config.ini')

list_metrics = list(config.items('metrics'))
list_metrics = [i[0] for i in list_metrics]
print("Metrics: ")
print(list_metrics)

list_hostnames = list(config.items('jmx_hostnames'))
list_hostnames = [i[0] for i in list_hostnames]
print("Hostnames:")
print(list_hostnames)

str_is_master = identify_master_node()
str_local_ip = get_local_ip()

print("Node is master node: " + str(str_is_master))
print("Local IP: " + str(str_local_ip))

file_name = "list_of_metrics"
create_file(file_name)

for hostname in list_hostnames:
    try:
        if "/" in hostname:
            if str_is_master:
                hostname = hostname.split(":")[0] + str(":16010")
            else:
                hostname = hostname.split(":")[0] + str(":16030")

        hostname = str_local_ip + ":" + hostname.split(":")[1]
        print("Hostname to be passed: " + hostname)

        obj_hbase_metrics = hbase_metrics(list_metrics)
        obj_hbase_metrics.initialize()
        obj_hbase_metrics.service_check()
        obj_hbase_metrics.fetch_and_append_metrics(hostname, file_name)
        obj_hbase_metrics.fetch_and_push_metrics(hostname)
    except Exception as e:
        print("Exception in fetching or pushing metrics: " + str(e))

# S3 metrics
list_tables = list(config.items('tables'))
list_tables = [i[0] for i in list_tables]
print("Tables")
print(list_tables)

if str_is_master:
    bucket_name = config['s3_metrics']['bucket']
    prefix = config['s3_metrics']['prefix']
    tag = config['s3_metrics']['tag']

    print("Pushing S3 Metrics: " + "Bucket name: " + bucket_name + " Prefix: " + prefix + "Tag: " + tag)

    try:
        obj_s3_metrics = s3_metrics()
        obj_s3_metrics.service_check()
        obj_s3_metrics.connect()
        obj_s3_metrics.fetch_and_push_metrics(bucket_name, prefix, tag, list_tables)
    except Exception as e:
        print(str(e))

#
# options = {
#     'statsd_host':'127.0.0.1',
#     'statsd_port':8125
# }
#
# initialize(**options)
#
# statsd.service_check(
#     check_name="HBaseMaster.service_check",
#     status="0",
#     message="Application is OK",
# )
#
# list_metrics = ["hbase.master.server.numRegionServers",
# "hbase.master.server.numDeadRegionServers",
# "hbase.regionserver.server.readRequestCount",
# "hbase.regionserver.server.writeRequestCount",
# "hbase.jvmmetrics.GcTimeMillis",
# "hbase.jvmmetrics.MemHeapUsedM",
# "hbase.jvmmetrics.MemHeapMaxM",
# "hbase.regionserver.server.compactionQueueLength",
# "hbase.regionserver.ipc.numCallsInGeneralQueue",
# "hbase.regionserver.server.Get_95th_percentile",
# "hbase.regionserver.io.FsPReadTime_95th_percentile",
# "hbase.regionserver.io.FsWriteTime_95th_percentile"]
#
# for metric in fetch_metrics("localhost:16030"):
#
#     if str(metric['metric']) in list_metrics:
#         print("METRIC:" + str(metric['metric']) + ": " + str(metric['value']))
#         statsd.gauge(metric['metric'], metric['value'],["{}:{}".format(k, v) for k, v in metric.get('tags', {}).items()])
