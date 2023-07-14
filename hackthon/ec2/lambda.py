import os
import uuid
from datetime import datetime, timedelta

import boto3
import pandas as pd

from hackthon.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, S3_DATA_BUCKET


def dump():
    NAMESPACE_METRIC = [('AWS/EC2', 'CPUUtilization'), ('CWAgent', 'mem_used_percent'),
                        ('AWS/EC2', 'NetworkIn'), ('AWS/EC2', 'NetworkOut'),
                        ('AWS/EC2', 'NetworkPacketsIn'), ('AWS/EC2', 'NetworkPacketsOut'),
                        ('AWS/EC2', 'CPUCreditUsage'), ('AWS/EC2', 'DiskReadOps'),
                        ('AWS/EC2', 'DiskWriteOps')]
    columns = ['cpu_utilization_percent', 'memory_used_percent', 'network_in_bytes',
               'network_out_bytes', 'network_packets_in_count',
               'network_packet_out_count', 'cpu_credit_usage', 'disk_read_ops', 'disk_write_ops', 'timestamp',
               'instance_type']
    __bucket = S3_DATA_BUCKET
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    ec2 = session.client('ec2')
    response = ec2.describe_instances()
    instance_tags = {}

    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            instance_id = instance['InstanceId']
            instance_type = instance['InstanceType']

            # Check if instance has tags
            if 'Tags' in instance:
                for tag in instance['Tags']:
                    key = tag['Key']
                    value = tag['Value']

                    # Check if tag name is "service"
                    if key == 'service':
                        # Add instance to dictionary
                        if value in instance_tags:
                            instance_tags[value].append((instance_id, instance_type))
                        else:
                            instance_tags[value] = [(instance_id, instance_type)]

    cloudwatch = session.client('cloudwatch')
    s3_client = session.client('s3')
    for key, value in instance_tags.items():
        for _value in value:
            df_list = []
            timestamp_df = []
            for metric in NAMESPACE_METRIC:
                metric_df = []

                end_time = datetime.utcnow()
                start_time = end_time - timedelta(hours=1)

                response = cloudwatch.get_metric_statistics(
                    Namespace=metric[0],
                    MetricName=metric[1],
                    Dimensions=[{'Name': 'InstanceId', 'Value': _value[0]}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=60,
                    Statistics=['Average'])

                metrics = list()
                for datapoint in response['Datapoints']:
                    timestamp = datapoint['Timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                    average_value = datapoint['Average']
                    metrics.append((timestamp, average_value))

                for _metric in metrics:
                    metric_df.append(_metric[1])
                    timestamp_df.append(_metric[0])
                df_list.append(metric_df)
            df_list.append(timestamp_df)
            df_list.append([_value[1]] * len(df_list[-1]))

            df = pd.DataFrame(df_list).T

            df.columns = columns
            remote_path = f'{key}/{uuid.uuid4()}.csv'
            file_name = remote_path.split('/')[-1]
            local_file_path = f'{os.getcwd()}/{file_name}.csv'
            df.to_csv(local_file_path, index=False)

            s3_client.upload_file(local_file_path, __bucket, remote_path)


dump()
