import configparser
import os


class ConfigOperator:

    def __init__(self, file_path=None):
        if file_path:
            self.config_path = file_path
        else:
            root_dir = os.path.dirname(os.path.abspath('..'))
            self.config_path = os.path.join(root_dir, "Config.ini")
        self.cf = configparser.ConfigParser()
        self.cf.read(self.config_path)

    def get_db(self, param):
        value = self.cf.get("Mysql-Database", param)
        return value

    def get_maoyan_film(self, param):
        value = self.cf.get("Maoyan-Film", param)
        return value

    def write_maoyan_film(self, param, content):
        self.cf.set("Maoyan-Film", param, content)
        self.cf.write(open(self.config_path, "w"))

    def get_douban_film(self, param):
        value = self.cf.get("Douban-Film", param)
        return value

    def write_douban_film(self, param, content):
        self.cf.set("Douban-Film", param, content)
        self.cf.write(open(self.config_path, "w"))
