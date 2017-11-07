import os
import boto3
import logging
from datetime import datetime,timezone,timedelta
import json
import urllib

logger = logging.getLogger()
logger.setLevel(logging.INFO)
OVER_RUN_TIME_VALUE = timedelta(seconds=28800)  # 8 hours
#OVER_RUN_TIME_VALUE = timedelta(seconds=500) # 500 seconds for testing

def check_over_running_EMR_cluster():
    client = boto3.client('emr')
    try:
        # List all cluster in running, starting, waiting status
        response = client.list_clusters(
        CreatedAfter=datetime(2015, 1, 1),
        ClusterStates=['RUNNING','STARTING','WAITING']
    )
        clusters = response["Clusters"]
        # check overruning clusters by comparing running time with the pre-defined value
        # running time is calculated by substracting current time to creation time
        i = 0
        overrunningClusters = []
        while i < len(clusters):
            cluster = clusters[i]
            creationTime = cluster["Status"]["Timeline"]["CreationDateTime"]
            runningTime = datetime.now(timezone.utc) - creationTime
            if runningTime > OVER_RUN_TIME_VALUE:
                overRunningCluster = {'Name': cluster["Name"], 'ID': cluster["Id"], 'Time': runningTime.total_seconds()}
                overrunningClusters.append(overRunningCluster)
            i+=1
        # if there are over-running clusters, post the clusters's details to slack for further examination
        if len(overrunningClusters) > 0:
            messageToPost = ":exclamation: There are " + str(len(overrunningClusters)) + " of over-running clusters."
            i = 0
            while i < len(overrunningClusters):
                overRunningCluster = overrunningClusters[i]
                messageToPost = messageToPost + "\n" + str(i+1) + ". Name: " + overRunningCluster["Name"] + "   ID: " + overRunningCluster["ID"] + "    Running time: " + str(int(round(overRunningCluster["Time"]))) + " seconds"
                i+=1
            post_to_slack(messageToPost)
    except Exception as e:
        logger.error("Request failed: %s", e)
        post_to_slack(":exclamation: There was something wrong with this lambda function! Please check the CloudWatch Logs!")

def check_failed_EMR_cluster():
    targetTime = datetime.today() - timedelta(days=1)
    client = boto3.client('emr')
    try:
        # List all cluster in failed status in one week
        response = client.list_clusters(
        CreatedAfter=datetime(targetTime.year, targetTime.month, targetTime.day),
        ClusterStates=['TERMINATED_WITH_ERRORS']
    )
        failedClusters = response["Clusters"]
        # if there are any failed cluster, post the clusters's detail to slack channel for further examination
        if len(failedClusters) > 0:
            messageToPost = ":exclamation: There are " + str(len(failedClusters)) + " of failed clusters."
            i = 0
            while i < len(failedClusters):
                failedCluster = failedClusters[i]
                clusterName = failedCluster["Name"]
                clusterID = failedCluster["Id"]
                errorCode = failedCluster["Status"]["StateChangeReason"]["Code"]
                errorMessage = failedCluster["Status"]["StateChangeReason"]["Message"]
                messageToPost = messageToPost + "\n" + str(i+1) + ". Name: " + clusterName + "   ID: " + clusterID + "    ErrorCode: " + errorCode + " \nErrorReason: " + errorMessage
                i+=1
            post_to_slack(messageToPost)
    
    except Exception as e:
        logger.error("Request failed: %s", e)
        post_to_slack(":exclamation: There was something wrong with this lambda function! Please check the CloudWatch Logs!")
    
        
def post_to_slack(messageText):
    slack_url='https://hooks.slack.com/services/xxxxxxxxxxxxxxxxxxxxx'; #slack channel for notification
    url_method= 'POST'
    request = urllib.request.Request(slack_url)
    request.add_header('Content-type', 'application/json')
    raw_json = {'text': messageText}
    json_data = json.dumps(raw_json).encode('utf-8')
    request.add_header('Content-Length', len(json_data))
    try:
        response = urllib.request.urlopen(request, json_data)
    except Exception as e:
        logger.error("Request failed: %s", e)

def lambda_handler(event, context):
    check_over_running_EMR_cluster()
    check_failed_EMR_cluster()
    #currentTime = datetime.today()
    # The function checking failed cluster run only once a week on Thursday despite of Lambda function run twice a week
    #if currentTime.weekday() == 3:
    #    check_failed_EMR_cluster()
    #else:
    #    logger.info("Today is not Thursday, so check_failed_EMR_cluster function is not going to be run today!")
