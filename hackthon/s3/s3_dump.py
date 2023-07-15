import boto3

from hackthon.settings import S3_DATA_BUCKET, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION


class S3Bucket:
    __BUCKET_NAME = S3_DATA_BUCKET

    def __init__(self):
        self.__bucket: str = self.__BUCKET_NAME
        self.session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
        self.s3_client = self.session.client('s3')

    def upload(self, local_file_path: str, csv_remote_file_path) -> None:
        self.s3_client.upload_file(local_file_path, self.__bucket, csv_remote_file_path)

    def list_files(self, prefix: str) -> list[str]:
        attr_list = self.s3_client.list_objects(Bucket=self.__bucket, Prefix=prefix, Delimiter='/').get('Contents')
        key_list: list[str] = list(map(lambda x: x['Key'], attr_list))  # type: ignore
        return key_list

    def get_object(self, key: str):
        return self.s3_client.get_object(Bucket=self.__bucket, Key=key)
