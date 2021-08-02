from datadog import initialize, statsd
from masterhbasemetrics import *


class hbase_metrics:
    def __init__(self, list_metrics):
        self.list_metrics = list_metrics

        self.options = {
            'statsd_host': '127.0.0.1',
            'statsd_port': 8125
        }

    def initialize(self):
        print("Initializing")
        initialize(**self.options)

    def service_check(self):
        print("Service Checks")
        statsd.service_check(
            check_name="HBaseMaster.service_check",
            status="0",
            message="Application is OK",
        )

    def fetch_and_append_metrics(self, hostname, file_name):
        print("Printing Metrics to file")

        for metric in fetch_metrics("http://" + str(hostname)):
            # print("Printing METRIC:" + str(metric['metric']) + ": " + str(metric['value']))
            f = open(file_name, "a")
            f.write(str(metric['metric']) + "\n")
            f.close()

    def fetch_and_push_metrics(self, hostname):
        print("Pushing Metrics")

        for metric in fetch_metrics("http://" + str(hostname)):
            if str(metric['metric']).lower() in self.list_metrics:
                print("Pushing METRIC:" + str(metric['metric']) + ": " + str(metric['value']))
                statsd.gauge(metric['metric'], metric['value'],
                             ["{}:{}".format(k, v) for k, v in metric.get('tags', {}).items()])
