import os
import logging
from great_expectations.data_context.store.tuple_store_backend import TupleStoreBackend

logger = logging.getLogger(__name__)


class CustomTupleS3StoreBackend(TupleStoreBackend):
    """
    Uses an S3 bucket as a store.

    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,
    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.
    The filepath_template is a string template used to convert the key to a filepath.
    """

    def __init__(
            self,
            bucket,
            prefix="",
            filepath_template=None,
            filepath_prefix=None,
            filepath_suffix=None,
            forbidden_substrings=None,
            platform_specific_separator=False,
            fixed_length_key=False
    ):
        super().__init__(
            filepath_template=filepath_template,
            filepath_prefix=filepath_prefix,
            filepath_suffix=filepath_suffix,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator,
            fixed_length_key=fixed_length_key
        )
        self.bucket = bucket
        self.prefix = prefix

    def _get(self, key):
        s3_object_key = os.path.join(
            self.prefix,
            self._convert_key_to_filepath(key)
        )

        s3 = self.get_s3('client')
        s3_response_object = s3.get_object(Bucket=self.bucket, Key=s3_object_key)
        return s3_response_object['Body'].read().decode(s3_response_object.get("ContentEncoding", 'utf-8'))

    def _set(self, key, value, content_encoding='utf-8', content_type='application/json'):
        s3_object_key = os.path.join(
            self.prefix,
            self._convert_key_to_filepath(key)
        )

        s3 = self.get_s3('resource')
        result_s3 = s3.Object(self.bucket, s3_object_key)
        if isinstance(value, str):
            result_s3.put(Body=value.encode(content_encoding), ContentEncoding=content_encoding,
                          ContentType=content_type)
        else:
            result_s3.put(Body=value, ContentType=content_type)
        return s3_object_key

    def list_keys(self):
        key_list = []

        s3 = self.get_s3('client')

        s3_objects = s3.list_objects(Bucket=self.bucket, Prefix=self.prefix)
        if "Contents" in s3_objects:
            objects = s3_objects["Contents"]
        elif "CommonPrefixes" in s3_objects:
            logger.warning("TupleS3StoreBackend returned CommonPrefixes, but delimiter should not have been set.")
            objects = []
        else:
            # No objects found in store
            objects = []

        for s3_object_info in objects:
            s3_object_key = s3_object_info['Key']
            s3_object_key = os.path.relpath(
                s3_object_key,
                self.prefix,
            )
            if self.filepath_prefix and not s3_object_key.startswith(self.filepath_prefix):
                continue
            elif self.filepath_suffix and not s3_object_key.endswith(self.filepath_suffix):
                continue
            key = self._convert_filepath_to_key(s3_object_key)
            if key:
                key_list.append(key)

        return key_list

    def get_url_for_key(self, key, protocol=None):
        location = os.environ['AWS_REGION']
        if location is None:
            location = "s3"
        else:
            location = "s3-" + location
        s3_key = self._convert_key_to_filepath(key)
        if not self.prefix:
            return f"https://{location}.amazonaws.com/{self.bucket}/{s3_key}"
        return f"https://{location}.amazonaws.com/{self.bucket}/{self.prefix}/{s3_key}"

    def remove_key(self, key):
        from botocore.exceptions import ClientError
        s3 = self.get_s3('resource')
        s3_key = self._convert_key_to_filepath(key)
        if s3_key:
            try:
                objects_to_delete = s3.meta.client.list_objects(Bucket=self.bucket, Prefix=self.prefix)

                delete_keys = {'Objects': []}
                delete_keys['Objects'] = [{'Key': k} for k in
                                          [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]
                s3.meta.client.delete_objects(Bucket=self.bucket, Delete=delete_keys)
                return True
            except ClientError as e:
                return False
        else:
            return False

    def _has_key(self, key):
        all_keys = self.list_keys()
        return key in all_keys

    @staticmethod
    def get_s3(key):
        s3 = None
        aws_flag = os.environ['AWS_FLAG']
        if aws_flag == 'NORMAL':
            import boto3
            if key == 'client':
                s3 = boto3.client('s3')
            elif key == 'resource':
                s3 = boto3.resource('s3')
        elif aws_flag == 'ASSUME':
            import boto3auth
            aws_account_id = os.environ['AWS_ACCOUNT_ID']
            aws_assume_role = os.environ['AWS_ASSUME_ROLE']
            aws_region = os.environ['AWS_REGION']
            b3a = boto3auth.Boto3Auth(region=aws_region, account_id=aws_account_id, role=aws_assume_role)
            b3a.creds()
            s3 = b3a.auth("s3", key)
        elif aws_flag == 'KEYS':
            import boto3
            aws_access_key = os.environ['AWS_ACCESS_KEY']
            aws_secret_key = os.environ['AWS_SECRET_KEY']
            if key == 'client':
                s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
            elif key == 'resource':
                s3 = boto3.resource('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
        else:
            import sys
            logger.error("Error getting $AWS_FLAG. Please make sure the environment variable exists with one of the "
                         "valid values (NORMAL, ASSUME or KEYS)")
            sys.exit("Error getting $AWS_FLAG. Please make sure the environment variable exists with one of the valid "
                     "values (NORMAL, ASSUME or KEYS)")
        return s3
