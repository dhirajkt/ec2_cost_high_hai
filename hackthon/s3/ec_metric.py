from datetime import datetime, timedelta

import boto3

from hackthon.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION


class EC2MetricCollector:

    def __init__(self, instance_id):
        self.session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
        self.cloudwatch = self.session.client('cloudwatch')
        self.instance_id = instance_id

    def get_metric(self, namespace, metric_name):
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=1)

        response = self.cloudwatch.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=[{'Name': 'InstanceId', 'Value': self.instance_id}],
            StartTime=start_time,
            EndTime=end_time,
            Period=60,
            Statistics=['Average'])

        metrics = list()
        for datapoint in response['Datapoints']:
            timestamp = datapoint['Timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            average_value = datapoint['Average']
            metrics.append((timestamp, average_value))
        return metrics
