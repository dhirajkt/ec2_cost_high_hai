import os
from typing import List
import pandas as pd
import uuid

from hackthon.ec2.ec2_instance import EC2Instance
from hackthon.s3.ec_metric import EC2MetricCollector
from hackthon.s3.s3_dump import S3Bucket


class MetricDump:
    NAMESPACE_METRIC: List[str] = [('AWS/EC2', 'CPUUtilization'), ('CWAgent', 'mem_used_percent'),
                                   ('AWS/EC2', 'NetworkIn'), ('AWS/EC2', 'NetworkOut'),
                                   ('AWS/EC2', 'NetworkPacketsIn'), ('AWS/EC2', 'NetworkPacketsOut'),
                                   ('AWS/EC2', 'CPUCreditUsage'),  ('AWS/EC2', 'DiskReadOps'),
                                   ('AWS/EC2', 'DiskWriteOps')]

    def __init__(self):
        self.columns = ['cpu_utilization_percent', 'memory_used_percent', 'network_in_bytes',
                        'network_out_bytes', 'network_packets_in_count',
                        'network_packet_out_count', 'cpu_credit_usage', 'disk_read_ops', 'disk_write_ops', 'timestamp',
                        'instance_type']
        self.instance_tag = EC2Instance().get_instance_tags()

    def dump(self):
        for key, value in self.instance_tag.items():
            for _value in value:
                df_list = []
                timestamp_df = []
                for metric in self.NAMESPACE_METRIC:
                    metric_df = []

                    metrics = EC2MetricCollector(instance_id=_value[0]).get_metric(
                        namespace=metric[0], metric_name=metric[1])
                    for _metric in metrics:
                        metric_df.append(_metric[1])
                        timestamp_df.append(_metric[0])
                    df_list.append(metric_df)
                df_list.append(timestamp_df)
                df_list.append([_value[1]] * len(df_list[-1]))

                df = pd.DataFrame(df_list).T

                df.columns = self.columns
                remote_path = f'{key}/{uuid.uuid4()}.csv'
                self.df_to_csv(df, remote_path)

    def df_to_csv(self, df, remote_path):
        file_name = remote_path.split('/')[-1]
        local_file_path = f'{os.getcwd()}/{file_name}.csv'
        df.to_csv(local_file_path, index=False)
        S3Bucket().upload(local_file_path=local_file_path, csv_remote_file_path=remote_path)
