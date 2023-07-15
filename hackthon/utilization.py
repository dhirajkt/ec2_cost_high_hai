import io
import re
from typing import Any

import pandas
from pandas import DataFrame, Series

from hackthon.s3.s3_dump import S3Bucket

df_instance = pandas.read_csv('Amazon_EC2_Instance_Comparison.csv')[
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
            return percentage * float(re.sub(' GiB', "", row['Instance Memory']))
    return None


class CalculateUtilization:
    def __init__(self, instance_id: str):
        self.instance_id: str = instance_id

    def calculate_metrics(self) -> DataFrame:
        bucket = S3Bucket()
        csv_file_keys: list[str] = [key for key in bucket.list_files(prefix=f'{self.instance_id}/')
                                    if key.endswith('.csv')]
        dfs: list[pandas.DataFrame] = []
        for key in csv_file_keys:
            csv_file = bucket.get_object(key=key)
            df = pandas.read_csv(io.BytesIO(csv_file['Body'].read()))
            dfs.append(df)
        metric_df: DataFrame = pandas.concat(dfs)
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
        return DataFrame.from_dict([calculated_metrics])
