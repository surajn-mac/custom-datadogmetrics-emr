import boto3
from datadog import initialize, statsd


class s3_metrics:
    def __init__(self):
        self.options = {
            'statsd_host': '127.0.0.1',
            'statsd_port': 8125
        }

        print("Initializing")
        initialize(**self.options)

    def service_check(self):
        statsd.service_check(
            check_name="HBaseMaster.service_check",
            status="0",
            message="Application is OK",
        )

    def connect(self):
        self.s3 = boto3.client('s3')

    def fetch_and_push_metrics(self, bucket_name, prefix, tag, table):
        # table=["magazine"]
        size = 0
        count = 0
        paginator = self.s3.get_paginator('list_objects_v2')
        for i in table:
            size = 0
            count = 0
            try:
                pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix + i + '/')
                for page in pages:
                    for obj in page['Contents']:
                        size += obj['Size']
                        count += 1
            except Exception as e:
                print("Exception when calculating size of tables: " + str(e))
                size = 0
            print("Pushing metric: " + str(i) + " Size:" + str(size) + " Count:" + str(count))
            statsd.gauge('aws_s3.tablesize', size, tags=[tag, "table:" + i])
            statsd.gauge('aws_s3.num_of_objects_in_table', count, tags=[tag, "table:" + i])
