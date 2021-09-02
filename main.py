import argparse
import subprocess
import time

from awsboto3 import *
from loggingInitializer import *

logging = initialize_logger("log")


def fetch_ip_address(cluster_id):
    # get boto3 emr client
    obj_awsBoto3 = awsBoto3()

    # fetch details of emr cluster
    list_ips = obj_awsBoto3.fetch_emr_details(cluster_id)

    # return ip addresses.
    return list_ips


def ssh_and_update_cron(list_ips, cluster_id):
    logging.info("===== ssh_and_update_cron =====" )
    logging.info("EMR IP list: " + str(list_ips))

    for i in list_ips:
        logging.info("====SSH into " + str(i) + "====")

        # ssh_cron_command = 'ssh ec2-user@172.30.138.8 "sudo bash -c \'touch /etc/cron.d/datadog-metrics; echo \\\"* * * * * root /usr/bin/ls / >> /tmp/datadog-metrics.log 2>&1\\\" >  /etc/cron.d/datadog-metrics; \'"'
        # ssh_cron_command = ' ssh ec2-user@172.30.138.8 "sudo bash -c \'ls / \' " '
        # ssh_cron_command = ' ssh ec2-user@' + i +' "sudo bash -c \'ifconfig \' " '
        # ssh_cron_command = 'ssh ec2-user@' + i + ' "sudo bash -c \'touch /etc/cron.d/datadog-metrics; echo \\\"* * * * * root cd /efs/users/datadog_agent/; /usr/bin/python3 /efs/users/datadog_agent/main.py ' + cluster_id + ' / >> /tmp/datadog-metrics.log 2>&1\\\" >  /etc/cron.d/datadog-metrics; \'"'
        ssh_cron_command = 'ssh ec2-user@' + i + ' "sudo bash -c \'touch /etc/cron.d/datadog-metrics; mkdir -p /var/log/datadog-custom-metrics/; echo \\\"* * * * * root cd /efs/users/datadog_agent/; /usr/bin/python3 /efs/users/datadog_agent/main.py ' + cluster_id + ' / >> /var/log/datadog-custom-metrics/datadog-metrics.log 2>&1\\\" >  /etc/cron.d/datadog-metrics; \'"'
        logging.info("SSH command " + ssh_cron_command)
        # Create a file with the command, and then try to execute the file
        f = open("ssh-configure-cron.sh", "w")
        f.write(
            ssh_cron_command
        )
        f.close()

        subprocess.call(['chmod', '0777', './ssh-configure-cron.sh'])
        # ssh ec2-user@172.30.138.8 "sudo bash -c 'touch /etc/cron.d/datadog-metrics; echo \"* * * * * root /usr/bin/ls / >> /tmp/datadog-metrics.log 2>&1\" > /etc/cron.d/datadog-metrics; ls /'"

        time.sleep(1)
        process = subprocess.Popen(
            """ 
            /bin/bash -c ./ssh-configure-cron.sh
            """,
            shell=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        output, stderr = process.communicate()
        status = process.poll()
        logging.info("Output of SSH command " + output)


def configure_ssh_in_emr(cluster_id):
    # Get all IP Addresses of a certain EMR Cluster
    logging.info("===== configure_ssh_in_emr =====" )
    logging.info("Cluster Id: " + str(cluster_id))
    list_ips = fetch_ip_address(cluster_id)

    # SSH into every IP Address and Add or Update the Cronfile
    ssh_and_update_cron(list_ips, cluster_id)


parser = argparse.ArgumentParser(description='Configure cron jobs on EMR Servers')
parser.add_argument('--cluster_id', '-c', type=str, help='EMR cluster ID')
args = parser.parse_args()
cluster_id = args.cluster_id

try:
    configure_ssh_in_emr(cluster_id)
except Exception as e:
    print(str(e))
    logging.info("Error: " + str(e))
