import glob
import json
import logging
import os
import time
import uuid

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy


class Cassandra:
    log = None

    def __init__(self, host, user_name, password, port, key_space, application_name, env_name,
                 cql_files_path, mode):
        self._host = host
        self._user_name = user_name
        self._password = password
        self._port = port
        self._key_space = key_space
        self._application_name = application_name
        self._env_name = env_name
        self._cql_files_path = cql_files_path
        self._mode = mode
        self._session = None
        self._file_path = None
        self._file_name = None
        self._migration_file_path = None
        self._scripts = None
        self._id = None
        self._version = None
        self._content = None
        self._up_scripts = []
        self._down_scripts = []
        self._migrations_table_name = "database_migrations"
        # self.log = self.instantiate_logger()

    @staticmethod
    def instantiate_logger():
        log = logging.getLogger()
        log.setLevel('DEBUG')
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        log.addHandler(handler)

        return log

    def establish_connection(self):
        auth_provider = PlainTextAuthProvider(username=self._user_name, password=self._password)
        cluster = Cluster(
            contact_points=self._host,
            port=self._port,
            auth_provider=auth_provider,
            max_schema_agreement_wait=300,
            control_connection_timeout=10,
            connect_timeout=30,
            load_balancing_policy=DCAwareRoundRobinPolicy()
        )
        self._session = cluster.connect()
        # self._session.default_consistency_level = ConsistencyLevel.ALL
        # self._session.default_serial_consistency_level = ConsistencyLevel.SERIAL
        # self._session.default_timeout = 120
        self._session.set_keyspace(self._key_space)

    def initiate_migration(self):
        if self._mode == "up":
            self.create_migration_file()
        elif self._mode == "down":
            self.migrate()

    def create_migration_file(self):
        self.create_migrations_table()
        self.get_file_names()
        self.execute_up_scripts()
        self.create_file_path()
        self.create_file_name()
        self.create_path()
        self.create_file()
        self.store_migration_details()

    def migrate(self):
        self.form_migrations_table()
        if self.get_rollback_data():
            self.execute_down_scripts()
            self.update_migration_table()
        else:
            print("No migrations to perform migration")

    def form_migrations_table(self):
        self._migrations_table_name += "_" + self._application_name + "_" + self._env_name

    def create_migrations_table(self):
        self.form_migrations_table()
        cql = """
            SELECT * FROM system_schema.tables
            where keyspace_name='{key_space}' and 
            table_name='{migrations_table}';
            """
        cql = cql.format(key_space=self._key_space, migrations_table=self._migrations_table_name)
        data = self._session.execute(cql).current_rows
        if not data:
            cql = """
                CREATE TABLE {table} (
                    id uuid,
                    applied_at timestamp,
                    version int,
                    name text,
                    content text,
                    PRIMARY KEY (id, version)
                ) WITH CLUSTERING ORDER BY (version DESC);
                """
            cql = cql.format(table=self._migrations_table_name)
            self._session.execute(cql)

    def get_file_names(self):
        self._scripts = glob.glob(
            os.path.join(os.path.join(os.path.abspath("scripts"), self._cql_files_path), "*.cql"))

    def execute_up_scripts(self):
        try:
            for script in self._scripts:
                script_name = script.split("\\")[-1]
                if not script_name.endswith("_rollback.cql"):
                    up_script = self.read_file(script)
                    self._up_scripts.append(up_script)
                    self._session.execute(up_script)
                    down_script = script_name.split(".cql")[0] + "_rollback.cql"
                    base_path = "\\".join(i for i in script.split("\\")[:len(script.split("\\")) - 1])
                    script = os.path.join(base_path, down_script)
                    down_script = self.read_file(script)
                    self._down_scripts.append(down_script)
            return True
        except Exception as exe:
            print("invalid script : " + script_name + "\nException : " + str(exe))

        return False

    def create_file_path(self):
        migrations_path = os.path.abspath("migrations")
        if not os.path.exists(migrations_path):
            os.mkdir(migrations_path)
        file_path = os.path.join(migrations_path, self._application_name)
        if not os.path.exists(file_path):
            os.mkdir(file_path)
        file_path = os.path.join(file_path, self._env_name)
        if not os.path.exists(file_path):
            os.mkdir(file_path)
        self._file_path = file_path

    def create_file_name(self):
        self._file_name = time.strftime(
            "%Y%m%d%H%M%S") + "_" + self._application_name + "_" + self._env_name + ".json"

    def create_path(self):
        self._migration_file_path = os.path.join(self._file_path, self._file_name)

    def create_file(self):
        json_data = {"data": []}
        for up_script, down_script in zip(self._up_scripts, self._down_scripts):
            json_data["data"].append({"up_script": up_script, "down_script": down_script})
        self._content = json.dumps(json_data)
        with open(self._migration_file_path, "w") as json_data_file:
            json.dump(json_data, json_data_file, ensure_ascii=False, indent=4)

    def store_migration_details(self):
        self.generate_id()
        self.generate_version()
        self.insert_data()

    def generate_id(self):
        self._id = uuid.uuid4()

    def generate_version(self):
        self._version = 1
        cql = """SELECT * FROM {table} limit 1;"""
        cql = cql.format(table=self._migrations_table_name)
        data = self._session.execute(cql).current_rows
        if data:
            version = data[0].version + 1
            self._version = version

    def insert_data(self):
        cql = """
            INSERT INTO {table}
            (id, version, name, content, applied_at)
            VALUES ({id}, {version}, '{name}', '{content}', toTimestamp(now()))
            """
        cql = cql.format(table=self._migrations_table_name, id=self._id,
                         version=self._version, name=self._file_name.strip(".json"), content=self._content)
        self._session.execute(cql)

    @staticmethod
    def read_file(script):
        data = None
        with open(script, "r") as fp:
            data = fp.read()
        return data

    def get_rollback_data(self):
        cql = """SELECT * FROM {table} limit 1;"""
        cql = cql.format(table=self._migrations_table_name)
        data = self._session.execute(cql).current_rows
        if data:
            self._down_scripts = [i["down_script"] for i in json.loads(data[0].content)["data"]]
            self._id = data[0].id
            return True
        return False

    def execute_down_scripts(self):
        for script in self._down_scripts[::-1]:
            self._session.execute(script)

    def update_migration_table(self):
        cql = """
            DELETE FROM {table} WHERE id={migration_id};
        """
        cql = cql.format(table=self._migrations_table_name, migration_id=self._id)
        self._session.execute(cql)


def main():
    c = Cassandra(host=["127.0.0.1"], user_name="admin", password="12345@qwert", port=9042, key_space="test_1",
                  application_name="app1", env_name="dev", cql_files_path="test_3", mode="down")
    c.establish_connection()
    c.initiate_migration()


if __name__ == "__main__":
    main()
