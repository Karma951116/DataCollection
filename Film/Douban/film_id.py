import requests
import os
import re
import time
import json

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from Utils import mysql_operator
from Utils import config_operator


class DoubanFilmIdCrawler:
    def __init__(self):
        self.base_url = 'https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=%E7%94%B5%E5%BD%B1&start=-&year_range=2020,2020'
        self.operator = mysql_operator.MysqlOperator()

    def get_response(self, start_num):
        # 伪装请求
        ua = UserAgent()
        proxies = {"https": "http://secondtransfer.moguproxy.com:9001"}
        headers = {
            'Accept': 'application/json, text/plain, */*',
            "Proxy-Authorization": 'Basic cjNZdnUxdG1wc0ZUbjVJOTpOS24wMlY0QnppSG5iYzc3',
            'Connection': 'keep-alive',
            #'Cookie': 'bid=iBOGkcNTJHk; douban-fav-remind=1; __gads=ID=d9e2cd1de24cbea2-2280b4974fc50043:T=1608807541:RT=1608807541:S=ALNI_MYdi3FkSNpbj1IN6S3E_QbXCFlguw; ll="108288"; _vwo_uuid_v2=DBCD16273FADD13F3B6942B79E4AEE394|f245c7a0158ac8300dee76cbd4e68bcf; __yadk_uid=wooQSeDU6NUP8oCYkDb7yWdCkEcvLPoA; __utmc=30149280; __utmc=223695111; __utmz=223695111.1612405929.11.10.utmcsr=search.douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/movie/subject_search; gadsTest=test; ap_v=0,6.0; __utma=223695111.845713901.1609918134.1612505096.1612508177.16; __utmb=223695111.0.10.1612508177; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1612508177%2C%22https%3A%2F%2Fsearch.douban.com%2Fmovie%2Fsubject_search%3Fsearch_text%3D%25E5%258D%2583%25E4%25B8%258E%25E5%258D%2583%25E5%25AF%25BB%26cat%3D1002%22%5D; _pk_ses.100001.4cf6=*; push_noty_num=0; push_doumail_num=0; __utma=30149280.889275231.1608807552.1612508177.1612509452.19; __utmz=30149280.1612509452.19.13.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utmb=30149280.1.10.1612509452; _pk_id.100001.4cf6=774be7c999102802.1609918134.16.1612511083.1612505096',
            'User-Agent': ua.random,
            'Referer': 'https://movie.douban.com/tag/'
        }
        cur_url = self.base_url.replace('-', str(start_num))
        response = requests.get(url=cur_url, headers=headers, proxies=proxies, verify=False, allow_redirects=False)
        if response.status_code is not 200:
            return 1
        return response.text

    def get_douban_filmid(self, html_doc):
        dict_out = json.loads(html_doc)
        list_data = dict_out['data']
        if len(list_data) <= 0:
            return None
        dict_name_id = {}
        for item in list_data:
            title = item['title']
            douban_id = item['id']
            dict_name_id[douban_id] = title

        return dict_name_id

    def writ_db(self, dict_name_id):
        for key in dict_name_id:
            result = ''
            sql = 'select * from douban_film_info where filmId=%d' % int(key)
            if self.operator.search(sql).__len__() > 0:
                result = '已存在'
            else:
                sql = 'insert into douban_film_info (filmId, filmName) values(%d, "%s")' % (int(key), dict_name_id[key])
                if self.operator.execute_sql(sql) == 0:
                    result = '成功'
                else:
                    result = '失败'
            print('%d;%s;%s' % (int(key), dict_name_id[key], result))


def official_method():
    crawler = DoubanFilmIdCrawler()
    cfo = config_operator.ConfigOperator()
    offset = int(cfo.get_douban_film('id_offset'))
    interval = int(cfo.get_douban_film('id_interval'))
    crawler.operator.conn_mysql()
    for num in range(offset, 10000, 20):
        try:
            html_doc = crawler.get_response(num)
            dict_name_id = crawler.get_douban_filmid(html_doc)
            if dict_name_id is None:
                break
            print(num)
            crawler.writ_db(dict_name_id)
            cfo.write_douban_film('id_offset', num.__str__())
        except Exception as e:
            while 1:
                try:
                    print('出现错误，30s后重试')
                    time.sleep(30)
                    cfo.write_douban_film('id_offset', num.__str__())
                    dict_name_id = crawler.get_douban_filmid(html_doc)
                    print(num)
                    crawler.writ_db(dict_name_id)
                    break
                except:
                    pass
        time.sleep(interval)
    crawler.operator.close_mysql()


if __name__ == '__main__':
    official_method()
