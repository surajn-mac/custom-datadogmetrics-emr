#!/usr/bin/env python3
import configparser
import argparse

from emrutils import *
from hbasemetrics import *
from masterhbasemetrics import *
from s3metrics import *
from loggingInitializer import *

logging = initialize_logger("log")
# check and set logging level
parser = argparse.ArgumentParser()
parser.add_argument('args', action='store', nargs='*', type=str, help='Arguments', metavar='args')
parser.add_argument('-l', action='store', nargs='?', type=int, help='Logging Level', metavar='logging_level')
logging_level = parser.parse_args().l
try:
    if logging_level:
        logging.setLoggerLevel(logging_level)
except Exception as exInvalidLoggingLevel:
    pass

config = configparser.ConfigParser(allow_no_value=True, delimiters=('='))
config.read('config.ini')

list_metrics = list(config.items('metrics'))
list_metrics = [i[0] for i in list_metrics]
logging.more_info("Metrics: "+ str(list_metrics))

list_tables = [table[0] for table in config.items('tables')]
# logging.more_info("Tables: " + str(list_tables))
list_renamed_metrics = [metric[1].replace('${tables}', table).lower()
                        for metric in config.items('renamed_metrics') for table in list_tables]
# logging.more_info("Renamed Metrics: " + str(list_renamed_metrics))
dict_renamed_metric_names = {metric[1].replace('${tables}', table).lower():metric[0].lower()
                             for metric in config.items('renamed_metrics') for table in list_tables}
# logging.more_info("Renamed Metric Names: " + str(dict_renamed_metric_names))
dict_renamed_metric_tables = {metric[1].replace('${tables}', table).lower():table
                             for metric in config.items('renamed_metrics') for table in list_tables}
# logging.more_info("Renamed Metric Tables: " + str(dict_renamed_metric_tables))

list_hostnames = list(config.items('jmx_hostnames'))
list_hostnames = [i[0] for i in list_hostnames]
# logging.more_info("Hostnames: "+ str(list_metrics))

str_is_master = identify_master_node()
str_local_ip = get_local_ip()

logging.info("Node is master node: " + str(str_is_master))
logging.info("Local IP: " + str(str_local_ip))

# file_name = "/tmp/" + "list_of_metrics"
# create_file(file_name)

for hostname in list_hostnames:
    try:
        if "/" in hostname:
            if str_is_master:
                hostname = hostname.split(":")[0] + str(":16010")
            else:
                hostname = hostname.split(":")[0] + str(":16030")

        hostname = str_local_ip + ":" + hostname.split(":")[1]
        #print("Hostname to be passed: " + hostname)
        logging.more_info("Hostname to be passed: " + hostname)

        obj_hbase_metrics = hbase_metrics(list_metrics)
        obj_hbase_metrics.initialize()
        obj_hbase_metrics.service_check()

        # if str_is_master:
        #     obj_hbase_metrics.fetch_and_append_metrics(hostname, file_name + "_master")
        # else:
        #     obj_hbase_metrics.fetch_and_append_metrics(hostname, file_name + "_region")

        obj_hbase_metrics.fetch_and_push_metrics(hostname)

        obj_hbase_renamed_metrics = hbase_metrics(list_renamed_metrics)
        obj_hbase_renamed_metrics.initialize()
        obj_hbase_renamed_metrics.service_check()
        obj_hbase_renamed_metrics.fetch_and_push_renamed_metrics(hostname, dict_renamed_metric_names,
                                                                 dict_renamed_metric_tables)

    except Exception as e:
        logging.info("Exception in fetching or pushing metrics for "+hostname+": " + str(e))

# S3 metrics
list_tables = list(config.items('tables'))
list_tables = [i[0] for i in list_tables]
# logging.info("Tables: "+ str(list_tables))
logging.info("===== Fetch and push s3 metrics =====")

if str_is_master:
    bucket_name = config['s3_metrics']['bucket']
    prefix = config['s3_metrics']['prefix']
    tag = config['s3_metrics']['tag']

    # logging.info("Pushing S3 Metrics: " + "Bucket name: " + bucket_name + " Prefix: " + prefix + "Tag: " + tag)

    try:
        obj_s3_metrics = s3_metrics()
        obj_s3_metrics.service_check()
        obj_s3_metrics.connect()
        obj_s3_metrics.fetch_and_push_metrics(bucket_name, prefix, tag, list_tables)
    except Exception as e:
        logging.info("Error: "+ str(e))

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
