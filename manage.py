from cassandra_migrate import Cassandra
from custom_logging import CustomLogging


def main():
    """

    :return:
    """
    config = {
        "host": ["127.0.0.1"],
        "user_name": "admin",
        "password": "12345@qwert",
        "port": 9042,
        "key_space": "test_1",
        "application_name": "app1",
        "env_name": "dev",
        "cql_files_path": "test_1",
        "mode": "down",
    }
    c = Cassandra(
        host=config["host"],
        user_name=config["user_name"],
        password=config["password"],
        port=config["port"],
        key_space=config["key_space"],
        application_name=config["application_name"],
        env_name=config["env_name"],
        cql_files_path=config["cql_files_path"],
        mode=config["mode"],
        logger=CustomLogging(application_name=config["application_name"], env_name=config["env_name"])
    )
    if c.establish_connection():
        c.initiate_migration()


if __name__ == "__main__":
    main()
