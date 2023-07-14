import os
from typing import List
import pandas as pd

from hackthon.s3.ec_metric import EC2MetricCollector
from hackthon.s3.s3_dump import S3Bucket
from hackthon.settings import INSTANCE_ID


class MetricDump:
    NAMESPACE_METRIC: List[str] = [('AWS/EC2', 'CPUUtilization'), ('CWAgent', 'mem_used_percent'),
                                   ('AWS/EC2', 'NetworkIn'), ('AWS/EC2', 'NetworkOut'),
                                   ('AWS/EC2', 'NetworkPacketsIn'), ('AWS/EC2', 'NetworkPacketsOut')]

    def __init__(self):
        self.columns = ['cpu_utilization_percent', 'cpu_utilization_timestamp', 'memory_used_percent',
                        'memory_used_timestamp', 'network_in_bytes', 'network_in_timestamp', 'network_out_bytes',
                        'network_out_timestamp', 'network_packets_in_count', 'network_packets_in_timestamp',
                        'network_packet_count']
        self.instance_id = INSTANCE_ID
        self.remote_path = f'manager_tag/{self.instance_id}/{self.instance_id}.csv'

    def dump(self):

        df_list = []
        for metric in self.NAMESPACE_METRIC:
            metric_df = []
            timestamp_df = []
            metrics = EC2MetricCollector(instance_id=self.instance_id).get_metric(
                namespace=metric[0], metric_name=metric[1])
            for _metric in metrics:
                metric_df.append(_metric[1])
                timestamp_df.append(_metric[0])
            df_list.append(metric_df)
            df_list.append(timestamp_df)

        df = pd.DataFrame(df_list).T
        df.columns = self.columns
        breakpoint()
        self.df_to_csv(df)

    def df_to_csv(self, df):
        local_file_path = f'{os.getcwd()}/{self.instance_id}.csv'
        df.to_csv(local_file_path, index=False)
        S3Bucket().upload(local_file_path=local_file_path, csv_remote_file_path=self.remote_path)
