"""
HBase Metrics Extraction

Fetches and parses HBase JMX metrics in JSON format and converts them to a list of tagged key=value metrics.
"""

import json
import logging
import re

import urllib3

logger = logging.getLogger('hbasemetrics')

hbase_regionserver_table_region_stat_pattern = re.compile("[Nn]amespace_(\w+)_table_(\w+)_region_(\w+)_metric_(\w+)")
ignored_region_metrics = {'replicaid'}


def gauge(metric, value, tags={}):
    return {
        'metric': metric,
        'value': value,
        'tags': tags,
    }


def parse_hadoop_bean_name(hadoop_bean_name):
    # expected format "Hadoop:service=HBase,name=Master,sub=Balancer", sub is optional
    d = dict(p.split('=') for p in hadoop_bean_name.split(','))
    return d['Hadoop:service'], d['name'], d.get('sub', None)


def aggregate_region_metric_values(metric, values):
    if re.match("(\w+)_(percentile|mean|median|max|min|99)", metric):
        return int(sum(values)) / int(len(values))
    elif re.match("(\w+)(Count|_num_ops|Size)", metric):
        values = [int(i) for i in values]
        return sum(values)
    elif metric not in ignored_region_metrics:
        logger.warn("WARN: can't aggregate metric: {}".format(metric))


def get_aggregate_region_metrics(prefix, bean):
    table_stats = {}
    for key, value in bean.items():
        if isinstance(value, int) or isinstance(value, float) or isinstance(value, str):
            region_metric_match = hbase_regionserver_table_region_stat_pattern.match(key)
            if region_metric_match:
                namespace, table, region, metric = region_metric_match.groups()
                if (namespace, table, metric) not in table_stats:
                    table_stats[(namespace, table, metric)] = []
                table_stats[(namespace, table, metric)].append(value)
    for (namespace, table, metric), values in table_stats.items():
        yield gauge("{}.region.{}".format(prefix, metric), aggregate_region_metric_values(metric, values), {
            "namespace": namespace,
            "table": table
        })


def get_raw_region_metrics(prefix, bean):
    for key, value in bean.items():
        if isinstance(value, int) or isinstance(value, float) or isinstance(value, str):
            region_metric_match = hbase_regionserver_table_region_stat_pattern.match(key)
            if region_metric_match:
                namespace, table, region, metric = region_metric_match.groups()
                yield gauge("{}.region.{}".format(prefix, metric), value, tags={
                    "namespace": namespace,
                    "table": table,
                    "region": region
                })


def get_metrics_from_bean(bean, aggregate_by_region):
    if bean['name'].startswith("Hadoop:service"):
        hadoop_service, name, sub = parse_hadoop_bean_name(bean['name'])
        prefix = ".".join([s.lower() for s in [hadoop_service, name, sub] if s is not None])
        if name == "RegionServer" and sub == "Regions":
            if aggregate_by_region:
                # then we want all metrics tagged by region as HBase gives them to us
                for metric in get_raw_region_metrics(prefix, bean):
                    yield metric
            else:
                # then we want to exclude the region tag from all region metrics, meaning we need to aggregate
                # by table across regions
                for metric in get_aggregate_region_metrics(prefix, bean):
                    yield metric

        else:
            for key, value in bean.items():
                if isinstance(value, int) or isinstance(value, float) or isinstance(value, str):
                    yield gauge("{}.{}".format(prefix, key), value)
                elif key == 'tag.isActiveMaster':
                    yield gauge("{}.alive".format(prefix, key), 1, tags={
                        "isActiveMaster": 'true' if value in {'True', 'true', True} else 'false'
                    })


def fetch_metrics(hbase_jmx_json_url, aggregate_by_region=False):
    http = urllib3.PoolManager()
    response = http.request("GET", hbase_jmx_json_url + "/jmx")
    data = json.loads(response.data.decode('utf-8'))
    for bean in data['beans']:
        for metric in get_metrics_from_bean(bean, aggregate_by_region):
            yield metric
