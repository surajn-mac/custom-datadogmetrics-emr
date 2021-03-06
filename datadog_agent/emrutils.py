import json
import socket
from loggingInitializer import *

logging = initialize_logger("Emrutils")

def identify_master_node():
    logging.info("==== identify_master_node =====")
    # check if server is master or slave
    f = open("/mnt/var/lib/info/instance.json", "r")
    # f = open("instance.json", "r")
    str_instance_details = f.read()
    json_instance_details = json.loads(str_instance_details)
    str_is_master = json_instance_details["isMaster"]
    logging.info("master node check: "+str(str_is_master))
    return str_is_master


def get_local_ip():
    logging.info("==== get_local_ip =====")
    hostname = socket.gethostname()
    IPAddr = socket.gethostbyname(hostname)
    print("Your Computer Name is:" + hostname)
    print("Your Computer IP Address is:" + IPAddr)
    logging.info("Your Computer Name is:" + hostname)
    logging.info("Your Computer IP Address is:" + IPAddr)
    return IPAddr


def create_file(file_name):
    logging.info("==== create_file =====")
    f = open(file_name, "w")
    f.close()
    logging.info(file_name+" file closed")
