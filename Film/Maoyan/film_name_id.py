# 获取猫眼影片Id，https://maoyan.com/films?showType=3&offset=0

import requests
import os
import re
import time

from bs4 import BeautifulSoup

from Utils import config_operator
from Utils import mysql_operator
from Utils import get_url


class FilmIdCrawler:

    def __init__(self):
        self.base_url = 'https://maoyan.com/films?showType=3&offset='
        self.operator = mysql_operator.MysqlOperator()

    def parse_reseponse(self, html_doc):
        # 这里一定要指定解析器，可以使用默认的html，也可以使用lxml比较快。
        bs = BeautifulSoup(html_doc, 'html.parser')
        # print(soup.prettify());channel-detail movie-item-title
        dict = {}
        for film in bs.find_all(class_='channel-detail movie-item-title'):
            film_name = film.find('a').string
            film_name = film_name.replace('\"', '\\\"')
            search_object = re.search(r'\d+', film.find('a').get('data-val'))
            film_id = search_object.group()
            dict[film_id] = film_name
        return dict

    def write_database(self, dict_name_id):
        for key, value in dict_name_id.items():
            # 查询是否已有
            sql = 'SELECT * from maoyan_filmid WHERE filmId="%d"' % int(key)
            if self.operator.search(sql).__len__() > 0:
                print('%s;%s;已存在' % (key, value))
                continue

            sql = 'INSERT INTO maoyan_filmid Set filmName="%s", filmId="%d"' % (value, int(key))
            if self.operator.execute_sql(sql) is 0:
                print('%s;%s;成功' % (key, value))
            else:
                print('%s;%s;失败' % (key, value))


def official_method():
    crawler = FilmIdCrawler()
    getor = get_url.GetUrl()
    cfo = config_operator.ConfigOperator()
    offset = int(cfo.get_maoyan_film('id_offset'))
    interval = int(cfo.get_maoyan_film('id_interval'))
    crawler.operator.conn_mysql()
    for offset in range(offset, 210, 30):
        try:
            response = getor.get_response(crawler.base_url + str(offset))
            dict_films = crawler.parse_reseponse(response.text)
            if dict_films.__len__() <= 0:
                break
            print(offset)
            crawler.write_database(dict_films)
            cfo.write_maoyan_film('id_offset', str(offset))
        except Exception as e:
            while 1:
                try:
                    print('出现异常，30s后重试\n' + str(e))
                    getor.change_account()
                    time.sleep(30)
                    response = getor.get_response(crawler.base_url + str(offset))
                    dict_films = crawler.parse_reseponse(response.text)
                    print(offset)
                    crawler.write_database(dict_films)
                    break
                except:
                    pass

        time.sleep(interval)


def test_method():
    crawler = FilmIdCrawler()
    file = open(os.path.dirname(os.getcwd()) + '\\Resources\\HtmlTempFile', mode='r')
    html_doc = file.read(-1)
    file.close()
    dict_films = crawler.parse_reseponse(html_doc)
    dict_dbresult = crawler.write_database(dict_films)


if __name__ == '__main__':
    # test_method()
    official_method()
    # single_page_method(1980)




