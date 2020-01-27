import environ


INI_RUNDECK = environ.secrets.INISecrets.from_path_in_env("APP_RUNDECK_SECRET")
INI_DB = environ.secrets.INISecrets.from_path_in_env("APP_DB_SECRET")


@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """

    @environ.config(prefix="PULSE")
    class RundeckConfig:
        """
        RundeckConfig class represeting the configuration to access
        rundeck API
        """
        host: str = INI_RUNDECK.secret(
            name="host", default=environ.var())
        token: str = INI_RUNDECK.secret(
            name="token", default=environ.var())

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
        table: str = environ.var("stg.rundeck_stat")
    rundeck = environ.group(RundeckConfig)
    db = environ.group(DBConfig)


def getConf():
    return environ.to_config(AppConfig)
