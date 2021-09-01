from datadog import initialize, statsd
from masterhbasemetrics import *
from loggingInitializer import *

logging = initialize_logger("hbasemertics")

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
        logging.logger.info("===== fetch_and_append_metrics =====")

        for metric in fetch_metrics("http://" + str(hostname)):
            #print("Printing METRIC:" + str(metric['metric']) + ": " + str(metric['value']))
            # logging.info("Printing METRIC:" + str(metric['metric']) + ": " + str(metric['value']))
            f = open(file_name, "a")
            f.write(str(metric['metric']) + "|" + str(hostname.split(":")[1]) + "\n")
            f.close()

    def fetch_and_push_metrics(self, hostname):
        logging.logger.info("===== fetch_and_push_metrics for " + hostname + " =====")
        for metric in fetch_metrics("http://" + str(hostname)):
            if str(metric['metric']).lower() in self.list_metrics:
                logging.much_more_info("Pushing METRIC:" + str(metric['metric']) + ": " + str(metric['value']))
                statsd.gauge(metric['metric'], metric['value'],
                             ["{}:{}".format(k, v) for k, v in metric.get('tags', {}).items()])

    def fetch_and_push_renamed_metrics(self, hostname, dict_renamed_metric_names, dict_renamed_metric_tables):
        logging.logger.info("===== fetch_and_push_renamed_metrics for " + hostname + " =====")
        for metric in fetch_metrics("http://" + str(hostname)):
            if str(metric['metric']).lower() in self.list_metrics:
                logging.much_more_info("Pushing METRIC:" + str(metric['metric']) + " as "
                             + dict_renamed_metric_names[str(metric['metric']).lower()] + ": " + str(metric['value']))
                statsd.gauge(dict_renamed_metric_names[str(metric['metric']).lower()], metric['value'],
                             ["{}:{}".format(k, v) for k, v in metric.get('tags', {}).items()]
                             + ["hbasetable:"+dict_renamed_metric_tables[str(metric['metric']).lower()]]
                             + ["flipboardemrprod"])
