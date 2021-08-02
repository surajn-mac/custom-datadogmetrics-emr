import json
import socket


def identify_master_node():
    # check if server is master or slave
    f = open("/mnt/var/lib/info/instance.json", "r")
    # f = open("instance.json", "r")
    str_instance_details = f.read()
    json_instance_details = json.loads(str_instance_details)
    str_is_master = json_instance_details["isMaster"]
    return str_is_master


def get_local_ip():
    hostname = socket.gethostname()
    IPAddr = socket.gethostbyname(hostname)
    print("Your Computer Name is:" + hostname)
    print("Your Computer IP Address is:" + IPAddr)
    return IPAddr


def create_file(file_name):
    f = open(file_name, "w")
    f.close()
