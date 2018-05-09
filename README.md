# Lambda function for monitoring abnormal EMR clusters

When starting an EMR cluster, it will take time for the cluster to booting up and setup all software that needed(Hadoop, Hive, Pig...). Sometime, the cluster failed to booting up due to bootstrap failure. 
Sometime a job run into an infinity loop and the cluster can not stop itself. The cluster's status remains “Running” but actually it will be like that forever you just throw the money out of the window!
![alt text](https://i.imgur.com/PsCjvjV.png)
The job is recoverable but I need something to detect and notify me whenever a cluster is getting errors and did not start successfully. So I decided to use CloudWatch Event rule + Lambda to create a cron job which check EMR clusters frequently.

In Lambda management console, create a new Lambda function.
For runtime enviroment, select Python 3.6.

Remember to create a role from IAM console to grant permission for the function.
![alt text](https://imgur.com/aWkShdI.png)
The function will need 2 roles below

```
AmazonElasticMapReduceReadOnlyAccess 
AWSLambdaBasicExecutionRole 
```

Next, Copy and paste the **abnormal-EMR-cluster-monitor.py** file to the Cloud9 IDE.
Let me explain something here.
The function has 3 main part:

* check_over_running_EMR_cluster(): check if any cluster is running longer than a given time(defined by OVER_RUN_TIME_VALUE)
* check_failed_EMR_cluster(): check if any cluster was failed to start(cluster in “TERMINATED_WITH_ERRORS” state)
* post_to_slack(messageText): function to post a message to Slack. I use Slack at work to get alarm and notification from all over the system

In CloudWatch Event rule, create a new rules to  start a cron job.

```
targetTime = datetime.today() - timedelta(days=1)
```

The check_failed_EMR_cluster() function only check clusters that run within a day ago so we will create a cron job that run once a day. You can change as you wish.
![alt text](https://i.imgur.com/CaVTODh.png)
Yeah, that's all.
