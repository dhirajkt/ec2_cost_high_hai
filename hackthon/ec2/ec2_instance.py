import boto3

from hackthon.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION

from dataclasses import dataclass


@dataclass
class EC2Specification:
    tag: str
    id: int
    type: str


class EC2Instance:

    def __init__(self):
        self.session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
        self.ec2 = self.session.client('ec2')

    def get_instance_tags(self):
        response = self.ec2.describe_instances()
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

        return instance_tags
