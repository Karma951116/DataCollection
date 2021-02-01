# 获取猫眼影片Id，https://maoyan.com/films?showType=3&offset=0

import requests
import os
import re
import time

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from Utils import mysql_operator

class FilmIdCrawler:

    def __init__(self):
        self.base_url = 'https://maoyan.com/films?showType=3&sortId=1&offset='
        self.min_offset = 0
        self.max_offset = 1980

    def get_response(self, offset):
        # 伪装请求
        ua = UserAgent()
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;'
                      'q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
            'Connection': 'keep-alive',
            'Cookie': '__mta=217831081.1608186388084.1611284360153.1611285352046.64; '
                      '_lxsdk_cuid=1766f5f7d67c8-0624e350695b8c-c791039-1fa400-1766f5f7d67c8; uuid_n_v=v1;'
                      ' recentCis=1%3D151%3D140%3D84%3D197; theme=moviepro;'
                      ' _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic;'
                      ' uuid=7B68F3905C8A11EBB5F151AF051690DE1D7402F0C99D456A8D6E3BB78EA2E993;'
                      ' _csrf=39b55a714339fd6eb8dd2bdf8b9601b85995049444885b7d2fca31e09257b05d;'
                      ' lt=t8I8mcKnX404DBn7MDrMn9T2PywAAAAAhgwAAIJf-Hmi1-ckw9XwgXMbD0YhmxgCBjMekOUcsBEaEcjR2Nt1V9nIMxa9JpQFElsHGA;'
                      ' lt.sig=t6O-xJMugn98A3qEGGWp7tdK3js; uid=1097461883; uid.sig=Qzn0f6oHqGvrFPGYvWuz1uoqSK0;'
                      ' _lxsdk=7B68F3905C8A11EBB5F151AF051690DE1D7402F0C99D456A8D6E3BB78EA2E993;'
                      ' Hm_lvt_703e94591e87be68cc8da0da7cbd0be2=1611282588,1611303543,1611303548,1611303555;'
                      ' Hm_lpvt_703e94591e87be68cc8da0da7cbd0be2=1611303776; '
                      '__mta=217831081.1608186388084.1611285352046.1611303775985.65;'
                      ' _lxsdk_s=177292c8bc8-464-60-d62%7C%7C82',
            'User-Agent': ua.random
        }
        cur_url = self.base_url + offset.__str__()
        response = requests.get(url=cur_url, headers=headers)
        return response.text

    def parse_reseponse(self, html_doc):
        # 这里一定要指定解析器，可以使用默认的html，也可以使用lxml比较快。
        bs = BeautifulSoup(html_doc, 'html.parser')
        # print(soup.prettify());channel-detail movie-item-title
        dict = {}
        for film in bs.find_all(class_='channel-detail movie-item-title'):
            film_name = film.find('a').string
            search_object = re.search(r'\d+', film.find('a').get('data-val'))
            film_id = search_object.group()
            dict[film_name] = film_id
        return dict

    def write_database(self, dict):
        operator = mysql_operator.MysqlOperator()
        if operator.conn_mysql() == 1:
            return
        dict_succ = {}
        dict_failed = {}
        for key, value in dict.items():
            # 查询是否已有
            sql = 'SELECT * from maoyan_filmid WHERE filmId="%d"' % int(value)
            if operator.search(sql).__len__() > 0:
                print('%s;%s;已存在' % (key, value))
                continue

            sql = 'INSERT INTO maoyan_filmid Set filmName="%s", filmId="%d"' % (key, int(value))
            if operator.execute_sql(sql) is 0:
                dict_succ[key] = value
                print('%s;%s;成功' % (key, value))
            else:
                dict_failed[key] = value
                print('%s;%s;失败' % (key, value))
        operator.close_mysql()
        dict_return = {'success': dict_succ, 'failed': dict_failed}
        return dict_return


def official_method():
    crawler = FilmIdCrawler()
    dict_success = {}
    dict_failed = {}
    failed_offset = []
    for offset in range(crawler.min_offset, crawler.max_offset, 30):
        html_doc = crawler.get_response(offset)
        dict_films = crawler.parse_reseponse(html_doc)
        if dict_films.__len__() <= 0:
            failed_offset.append(offset)
            continue
        print('Offset:%d' % offset)
        dict_dbresult = crawler.write_database(dict_films)
        #dict_success.update(dict_dbresult['success'])
        #dict_failed.update(dict_dbresult['failed'])
        time.sleep(55)
    #log(dict_success, dict_failed, failed_offset)


def single_page_method(offset):
    crawler = FilmIdCrawler()
    html_doc = crawler.get_response(offset)
    # print(html_doc)
    dict_films = crawler.parse_reseponse(html_doc)
    if dict_films.__len__() <= 0:
        return
    print('Offset:%d' % offset)
    dict_dbresult = crawler.write_database(dict_films)
    time.sleep(55)


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




