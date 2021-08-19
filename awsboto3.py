import boto3
from loggingInitializer import *

logging = initialize_logger("log")


class awsBoto3:
    def __init__(self):
        self.client = boto3.client('emr', region_name="us-east-1")

    def fetch_emr_details(self, cluster_id):
        logging.info("===== fetch_emr_details =====" )
        self.cluster_id = cluster_id
        try:
            response = self.client.list_instances(
                ClusterId=self.cluster_id,
                InstanceStates=[
                    'RUNNING',
                ],
            )
            # Get list of ip addresses.
            self.list_ips = []

            #print(response['Instances'][len(list_intance_resp['Instances']) - 1]['PrivateIpAddress'])
            logging.info("Response: "+ str(response['Instances'][len(list_intance_resp['Instances']) - 1]['PrivateIpAddress']))
            for i in range(12):
                self.list_ips.append(response["Instances"][i]["PrivateIpAddress"])

            return self.list_ips
        except Exception as e:
            print(str(e))
            logging.info("Error: "+ str(e))
