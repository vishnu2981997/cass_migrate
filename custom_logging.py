"""

"""
import datetime
import logging
import time
import os


class CustomLogging:

    def __init__(self, application_name, env_name, mode):
        self._application_name = application_name
        self._env_name = env_name
        self._mode = mode
        self._logs_dir = "cassandra_migrate_logging"
        self._time_stamp = time.strftime("%Y%m%d%H%M%S")
        self._file_name = "{0}_{1}_{2}_{3}".format(str(self._time_stamp), self._application_name,
                                                   self._env_name, self._mode)
        self._log_path = self.create_log_file()
        logging.basicConfig(filename=os.path.join(self._log_path, self._file_name + ".log"), level=logging.INFO)

    def create_log_file(self):
        logs_path = os.path.abspath(self._logs_dir)
        if not os.path.exists(logs_path):
            os.mkdir(logs_path)
        file_path = os.path.join(logs_path, self._application_name)
        if not os.path.exists(file_path):
            os.mkdir(file_path)
        file_path = os.path.join(file_path, self._env_name)
        if not os.path.exists(file_path):
            os.mkdir(file_path)

        return file_path

    @staticmethod
    def log(msg="", error=None):
        """
        logs the given msg to the specified log file
        :param msg: string
        :param error: string
        :return: None
        """
        time_stamp = time.time()
        content = datetime.datetime.fromtimestamp(time_stamp).strftime("%Y-%m-%d %H:%M:%S")
        content += " " + str(msg)
        if error:
            content += " " + str(error)
        logging.info(content)
