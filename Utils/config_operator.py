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
        with open(self.config_path, "w") as f:
            self.cf.write(f)
            f.close()

    def get_douban_film(self, param):
        value = self.cf.get("Douban-Film", param)
        return value

    def write_douban_film(self, param, content):
        self.cf.set("Douban-Film", param, content)
        with open(self.config_path, "w") as f:
            self.cf.write(f)
            f.close()

    def get_baidu_celebrity(self, param):
        value = self.cf.get("Baidu-Celebrity", param)
        return value

    def write_baidu_celebrity(self, param, content):
        self.cf.set("Baidu-Celebrity", param, content)
        with open(self.config_path, "w") as f:
            self.cf.write(f)
            f.close()
