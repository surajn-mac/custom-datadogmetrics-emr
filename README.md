# custom-datadogmetrics-emr
Configure Custom Metrics in EMR  

This script willbe triggered from Jenkins Job: Mactores-EMR-Datadog-config  

Usage: python main.py -c EMR_CLUSTER_ID  

Script Details  
1. Datagog Agent  
The datadog-agent folder that contains the custom metrics in config.ini file will be copied on the efs location  

2. SSH into EMR  
The script will SSH into every Server of the EMR cluster and configure a cronjob at the location of /etc/cron.d  


