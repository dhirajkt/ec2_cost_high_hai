from typing import Any

import pandas
from pandas import DataFrame, Series
import re

df_instance = pandas.read_csv('/Users/kishan.tripathi/Desktop/hackathon/data/Amazon EC2 Instance Comparison.csv')[
    ['API Name', 'Instance Memory']]


def avg(arr: Series):
    arr = arr.dropna()
    if len(arr) == 0:
        return None
    return sum(arr) / len(arr)


def top_n(arr: Series, n: int = 10) -> float | Any:
    arr = arr.dropna()
    if len(arr) == 0:
        return None
    length = len(arr)
    return arr.sort_values(ascending=False).head(int(length * n / 100)).mean()


def memory_in_gbs(percentage: float, instance_type: str):
    for index, row in df_instance.iterrows():
        if row['API Name'].__eq__(instance_type):
            return percentage * float(re.sub(' GiB', "",row['Instance Memory']))
    return None


class CalculateUtilization:
    def __init__(self, instance_id: str):
        self.instance_id: str = instance_id
        self.remote_path = f'/Users/kishan.tripathi/Desktop/hackathon/data/metrics.csv'

    def calculate_metrics(self) -> dict[str, float]:
        metric_df: DataFrame = pandas.read_csv(self.remote_path)
        calculated_metrics: dict[str, float] = {}
        calculated_metrics['avg_cpu_utilization'] = avg(metric_df['cpu_utilization_percent'])
        calculated_metrics['top_10_percent_cpu'] = top_n(metric_df['cpu_utilization_percent'])
        calculated_metrics['avg_memory_used'] = avg(metric_df['memory_used_percent'])
        calculated_metrics['avg_memory_used_in_gb'] = memory_in_gbs(calculated_metrics['avg_memory_used'],
                                                                    metric_df['instance_type'])
        calculated_metrics['peak_memory_usage'] = metric_df['memory_used_percent'].max()
        calculated_metrics['top_10_percent_memory'] = top_n(metric_df['memory_used_percent'])
        calculated_metrics['network_in_average'] = avg(metric_df['network_in_bytes'])
        calculated_metrics['network_out_average'] = avg(metric_df['network_out_bytes'])
        calculated_metrics['network_packet_in_average'] = avg(metric_df['network_packets_in_count'])
        calculated_metrics['network_packet_out_average'] = avg(metric_df['network_packet_out_count'])
        calculated_metrics['cpu_credit_usage'] = metric_df['cpu_credit_usage'].max()
        calculated_metrics['disk_read_ops_average'] = avg(metric_df['disk_read_ops'])
        calculated_metrics['disk_write_ops_average'] = avg(metric_df['disk_write_ops'])

        print(calculated_metrics)
        return calculated_metrics
