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

    def get_film_baseinfo(self, param):
        value = self.cf.get("Film-BaseInfo", param)
        return value

    def write_film_baseinfo(self, param, content):
        self.cf.set("Film-BaseInfo", param, content)
        self.cf.write(open(self.config_path, "w"))

    def get_film_boxoffice(self, param):
        value = self.cf.get("Film-BoxOffice", param)
        return value

    def write_film_boxoffice(self, param, content):
        self.cf.set("Film-BoxOffice", param, content)
        self.cf.write(open(self.config_path, "w"))

