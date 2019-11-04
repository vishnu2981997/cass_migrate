from cass_migrate import Cassandra
from custom_logging import CustomLogging
import sys


def main():
    """

    :return:
    """
    # config = {
    #     "host": ["127.0.0.1"],
    #     "user_name": "admin",
    #     "password": "12345@qwert",
    #     "port": 9042,
    #     "key_space": "test_1",
    #     "application_name": "app1",
    #     "env_name": "dev",
    #     "cql_files_path": "test_1",
    #     "mode": "up",
    # }
    config = {
        "host": [sys.argv[1]],
        "user_name": sys.argv[2],
        "password": sys.argv[3],
        "port": int(sys.argv[4]),
        "key_space": sys.argv[5],
        "application_name": sys.argv[6],
        "env_name": sys.argv[7],
        "mode": sys.argv[8],
        "rollback_version" : None
    }
    if config["mode"] == "up":
        config["cql_files_path"] = sys.argv[9]
    elif config["mode"] == "down":
        if len(sys.argv) == 10:
            config["rollback_version"] = int(sys.argv[9])
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
        logger=CustomLogging(
            application_name=config["application_name"],
            env_name=config["env_name"],
            mode=config["mode"]
        ),
        rollback_version=config["rollback_version"]
    )
    if c.establish_connection():
        c.initiate_migration()


if __name__ == "__main__":
    main()
