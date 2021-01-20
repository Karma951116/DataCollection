import configparser
import os

class ReadConfig:

    def __init__(self, filepath=None):
        if filepath:
            configpath = filepath
        else:
            root_dir = os.path.dirname(os.path.abspath('.'))
            configpath = os.path.join(root_dir, "Config.ini")
        self.cf = configparser.ConfigParser()
        self.cf.read(configpath)

    def get_db(self, param):
        value = self.cf.get("Mysql-Database", param)
        return value

