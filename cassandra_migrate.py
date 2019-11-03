import glob
import json
import os
import time
import uuid

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy


class Cassandra:
    """"""

    def __init__(self, host, user_name, password, port, key_space, application_name, env_name,
                 cql_files_path, mode, logger):
        """

        :param host:
        :param user_name:
        :param password:
        :param port:
        :param key_space:
        :param application_name:
        :param env_name:
        :param cql_files_path:
        :param mode:
        :param logger:
        """
        self._host = host
        self._user_name = user_name
        self._password = password
        self._port = port
        self._key_space = key_space
        self._application_name = application_name
        self._env_name = env_name
        self._cql_files_path = cql_files_path
        self._mode = mode
        self._log = logger
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
        self._success_scripts = []
        self._migrations_table_name = "database_migrations"

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, host):
        self._host = host

    @host.deleter
    def host(self):
        del self._host

    @property
    def user_name(self):
        return self._user_name

    @user_name.setter
    def user_name(self, user_name):
        self._user_name = user_name

    @user_name.deleter
    def user_name(self):
        del self._user_name

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, password):
        self._password = password

    @password.deleter
    def password(self):
        del self._password

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, port):
        self._port = port

    @port.deleter
    def port(self):
        del self._port

    @property
    def key_space(self):
        return self._key_space

    @key_space.setter
    def key_space(self, key_space):
        self._key_space = key_space

    @key_space.deleter
    def key_space(self):
        del self._key_space

    @property
    def application_name(self):
        return self._application_name

    @application_name.setter
    def application_name(self, application_name):
        self._application_name = application_name

    @application_name.deleter
    def application_name(self):
        del self._application_name

    @property
    def env_name(self):
        return self._env_name

    @env_name.setter
    def env_name(self, env_name):
        self._env_name = env_name

    @env_name.deleter
    def env_name(self):
        del self._env_name

    @property
    def cql_files_path(self):
        return self._cql_files_path

    @cql_files_path.setter
    def cql_files_path(self, cql_files_path):
        self._cql_files_path = cql_files_path

    @cql_files_path.deleter
    def cql_files_path(self):
        del self._cql_files_path

    @property
    def mode(self):
        return self._mode

    @mode.setter
    def mode(self, mode):
        self._mode = mode

    @mode.deleter
    def mode(self):
        del self._mode

    def establish_connection(self):
        """

        :return:
        """
        self._log.log("establishing connection with config : " + json.dumps(self.__repr__()))
        try:
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
            self._session.set_keyspace(self._key_space)
            self._log.log("connection established")
            return True
        except Exception as exe:
            self._log.log("Invalid credentials or smthin went wrong", error=str(exe))
        return False

    def initiate_migration(self):
        """

        :return:
        """
        try:
            success = False
            self._log.log("migration initiated mode : " + self._mode)
            if self._mode == "up":
                success = self.create_migration()
            elif self._mode == "down":
                success = self.migrate()
            if success:
                self._log.log("migration success mode : " + self._mode)
        except Exception as exe:
            self._log.log(error=exe)

    def create_migration(self):
        """

        :return:
        """
        if self.create_migrations_table():
            if self.get_file_names():
                if self.execute_up_scripts():
                    self.create_file_path()
                    self.create_file_name()
                    self.create_path()
                    self.create_file()
                    self.store_migration_details()
                    return True
                else:
                    self.exception_rollback()
        return False

    def migrate(self):
        """

        :return:
        """
        self.form_migrations_table()
        if self.get_rollback_data():
            if self.execute_down_scripts():
                self.update_migration_table()
                return True
            else:
                self.exception_rollback()
        return False

    def form_migrations_table(self):
        """

        :return:
        """
        self._log.log("forming migrations table name")
        self._migrations_table_name += "_" + self._application_name + "_" + self._env_name

    def create_migrations_table(self):
        """

        :return:
        """
        self._log.log("creating migration table")
        try:
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
            return True
        except Exception as exe:
            self._log.log(error=exe)
        return False

    def get_file_names(self):
        """

        :return:
        """
        self._log.log("fetching file names")
        self._scripts = glob.glob(
            os.path.join(os.path.join(os.path.abspath("scripts"), self._cql_files_path), "*.cql"))
        if self._scripts:
            return True
        else:
            self._log.log("No scripts in the specified path or path might be wrong")
            return False

    def execute_up_scripts(self):
        """

        :return:
        """
        self._log.log("executing up scripts")
        script_name = None
        try:
            for script in self._scripts:
                script_name = script.split("\\")[-1]
                if not script_name.endswith("_rollback.cql"):
                    up_script = self.read_file(script)
                    self._up_scripts.append(up_script)
                    self._session.execute(up_script)
                    self._success_scripts.append(up_script)
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
        """

        :return:
        """
        self._log.log("creating file path")
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
        """

        :return:
        """
        self._log.log("creating file name")
        self._file_name = time.strftime(
            "%Y%m%d%H%M%S") + "_" + self._application_name + "_" + self._env_name + ".json"

    def create_path(self):
        """

        :return:
        """
        self._log.log("creating path")
        self._migration_file_path = os.path.join(self._file_path, self._file_name)

    def create_file(self):
        """

        :return:
        """
        self._log.log("creating file")
        json_data = {"data": []}
        for up_script, down_script in zip(self._up_scripts, self._down_scripts):
            json_data["data"].append({"up_script": up_script, "down_script": down_script})
        self._content = json.dumps(json_data)
        with open(self._migration_file_path, "w") as json_data_file:
            json.dump(json_data, json_data_file, ensure_ascii=False, indent=4)

    def store_migration_details(self):
        """

        :return:
        """
        self._log.log("storing migration details")
        self.generate_id()
        if self.generate_version():
            self.insert_data()

    def generate_id(self):
        """

        :return:
        """
        self._log.log("generating id")
        self._id = uuid.uuid4()

    def generate_version(self):
        """

        :return:
        """
        self._log.log("generating version")
        try:
            self._version = 1
            cql = """SELECT * FROM {table} limit 1;"""
            cql = cql.format(table=self._migrations_table_name)
            data = self._session.execute(cql).current_rows
            if data:
                version = data[0].version + 1
                self._version = version
            return True
        except Exception as exe:
            self._log.log(error=exe)
        return False

    def insert_data(self):
        """

        :return:
        """
        self._log.log("inserting data into migration table")
        try:
            cql = """
                INSERT INTO {table}
                (id, version, name, content, applied_at)
                VALUES ({id}, {version}, '{name}', '{content}', toTimestamp(now()))
                """
            cql = cql.format(table=self._migrations_table_name, id=self._id,
                             version=self._version, name=self._file_name.strip(".json"), content=self._content)
            self._session.execute(cql)
            return True
        except Exception as exe:
            self._log.log(error=exe)
        return False

    @staticmethod
    def read_file(script):
        """

        :param script:
        :return:
        """
        with open(script, "r") as fp:
            data = fp.read()
        return data

    def get_rollback_data(self):
        """

        :return:
        """
        self._log.log("fetching rollback data")
        try:
            cql = """SELECT * FROM {table} limit 1;"""
            cql = cql.format(table=self._migrations_table_name)
            data = self._session.execute(cql).current_rows
            if data:
                self._down_scripts = [i["down_script"] for i in json.loads(data[0].content)["data"]]
                self._id = data[0].id
                return True
            else:
                self._log.log("No migrations to perform migration")
                return False
        except Exception as exe:
            self._log.log(error=exe)
        return False

    def execute_down_scripts(self):
        """

        :return:
        """
        self._log.log("executing down scripts")
        try:
            for script in self._down_scripts[::-1]:
                self._session.execute(script)
                self._success_scripts.append(script)
            return True
        except Exception as exe:
            self._log.log(error=exe)
        return False

    def update_migration_table(self):
        """

        :return:
        """
        self._log.log("updating migration table")
        try:
            cql = """
                DELETE FROM {table} WHERE id={migration_id};
            """
            cql = cql.format(table=self._migrations_table_name, migration_id=self._id)
            self._session.execute(cql)
            return True
        except Exception as exe:
            self._log.log(error=exe)
        return False

    def exception_rollback(self):
        """

        :return:
        """
        for script in self._success_scripts[::-1]:
            self._session.execute(script)

    def __repr__(self):
        return {
            "host": self._host,
            "user_name": self._user_name,
            "password": self._password,
            "port": self._port,
            "key_space": self._key_space,
            "application_name": self._application_name,
            "env_name": self._env_name,
            "cql_files_path": self._cql_files_path,
            "mode": self._mode,
        }

    def __str__(self):
        string = "Auth(host = {0}, user_name = {1}, password = {2}, " \
                 "port = {3}, key_space = {4}, application_name = {5}, " \
                 "env_name = {6}, cql_files_path = {7}, mode = {8})"
        return string.format(self._host, self._user_name, self._password,
                             self._port, self._key_space, self._application_name,
                             self._env_name, self._cql_files_path, self._mode)
