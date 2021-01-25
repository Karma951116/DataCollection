import pymysql
from Utils.read_config import ReadConfig


class MysqlOperator:

    def __init__(self):
        self.data = ReadConfig()

    def conn_mysql(self):
        # 创建一个连接数据库的对象
        try:
            host = self.data.get_db("host")
            user = self.data.get_db("user")
            password = self.data.get_db("password")
            db = self.data.get_db("database")
            charset = self.data.get_db("charset")
            self.conn = pymysql.connect(host=host, user=user, password=password, db=db, charset=charset)
            self.cur = self.conn.cursor()
        except:
            print("数据库连接失败")
            return 1
        return 0

    # 查询
    def search(self, sql):
        result = None
        try:
            self.cur.execute(sql)
            # result = self.cur.fetchone()  # 使用 fetchone()方法获取单条数据.只显示一行结果
            result = self.cur.fetchall()  # 显示所有结果
        except:
            #print("执行查询语句失败 %s" % sql)
            return 1

        return result

    # 增删改
    def execute_sql(self, sql):
        try:
            self.cur.execute(sql)  # 执行sql
            self.conn.commit()  # 增删改操作完数据库后，需要执行提交操作
        except:
            # 发生错误时回滚
            self.conn.rollback()
            #print("执行语句失败 %s" % sql)
            return 1
        return 0

    def close_mysql(self):
        self.cur.close()
        self.conn.close()
