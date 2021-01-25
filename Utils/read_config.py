import configparser
import os


class ReadConfig:

    def __init__(self, file_path=None):
        if file_path:
            config_path = file_path
        else:
            root_dir = os.path.dirname(os.path.abspath('..'))
            config_path = os.path.join(root_dir, "Config.ini")
        self.cf = configparser.ConfigParser()
        self.cf.read(config_path)

    def get_db(self, param):
        value = self.cf.get("Mysql-Database", param)
        return value

