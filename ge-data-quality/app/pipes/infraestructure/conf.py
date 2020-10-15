import os
import environ

INI_DB = environ.secrets.INISecrets.from_path_in_env("APP_DB_SECRET")
INI_AWS = environ.secrets.INISecrets.from_path_in_env("APP_AWS_SECRET")

@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """

    @environ.config(prefix="DB")
    class DBConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        host: str = INI_DB.secret(name="host", default=environ.var())
        port: int = INI_DB.secret(name="port", default=environ.var())
        name: str = INI_DB.secret(name="dbname", default=environ.var())
        user: str = INI_DB.secret(name="user", default=environ.var())
        password: str = INI_DB.secret(name="password", default=environ.var())

    @environ.config(prefix="AWS")
    class AWSConfig:
        """
        AWSConfig Class representing the configuration to access the aws account
        """
        access_key:        str = INI_AWS.secret(name="AWS_ACCESS_KEY_ID", default=environ.var())
        secret_access_key: str = INI_AWS.secret(name="AWS_SECRET_ACCESS_KEY", default=environ.var())
        s3_bucket:         str = INI_AWS.secret(name="AWS_S3_BUCKET_DOCS", default=environ.var())
        s3_prefix_valid:   str = INI_AWS.secret(name="AWS_S3_PREFIX_VALID", default=environ.var())

    db = environ.group(DBConfig)
    aws = environ.group(AWSConfig)

def set_conf():
    config = environ.to_config(AppConfig)
    os.environ['dw_host'] = config.db.host
    os.environ['dw_port'] = config.db.port
    os.environ['dw_dbname'] = config.db.name
    os.environ['dw_user'] = config.db.user
    os.environ['dw_password'] = config.db.password
    os.environ['AWS_ACCESS_KEY_ID'] = config.aws.access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.aws.secret_access_key
    os.environ['S3_BUCKET_DOCS'] = config.aws.s3_bucket
    os.environ['S3_PREFIX_VALIDATIONS'] = config.aws.s3_prefix_valid
    os.environ['SLACK_WEBHOOK'] = 'https://hooks.slack.com/services/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'