import requests
import os
import re
import time
import json

from bs4 import BeautifulSoup
from fake_useragent import UserAgent


from Utils import mysql_operator
from Utils import config_operator
from Utils import get_url


class DoubanFilmInfoCrawler:
    def __init__(self):
        self.base_url = 'https://movie.douban.com/subject/-/'
        self.operator = mysql_operator.MysqlOperator()

    def get_response(self, film_id):
            # 伪装请求
            ua = UserAgent()
            headers = {
                'Connection': 'keep-alive',
                'Cookie': 'gadsTest=test; bid=iBOGkcNTJHk; douban-fav-remind=1; __gads=ID=d9e2cd1de24cbea2-2280b4974fc50043:T=1608807541:RT=1608807541:S=ALNI_MYdi3FkSNpbj1IN6S3E_QbXCFlguw; ll="108288"; _vwo_uuid_v2=DBCD16273FADD13F3B6942B79E4AEE394|f245c7a0158ac8300dee76cbd4e68bcf; __yadk_uid=wooQSeDU6NUP8oCYkDb7yWdCkEcvLPoA; __utmc=30149280; __utmc=223695111; ap_v=0,6.0; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1612405929%2C%22https%3A%2F%2Fsearch.douban.com%2Fmovie%2Fsubject_search%3Fsearch_text%3D%25E5%258D%2583%25E4%25B8%258E%25E5%258D%2583%25E5%25AF%25BB%26cat%3D1002%22%5D; __utma=30149280.889275231.1608807552.1612149625.1612405929.13; __utmz=30149280.1612405929.13.12.utmcsr=search.douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/movie/subject_search; __utma=223695111.845713901.1609918134.1612149640.1612405929.11; __utmz=223695111.1612405929.11.10.utmcsr=search.douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/movie/subject_search; gadsTest=test; _pk_id.100001.4cf6=774be7c999102802.1609918134.11.1612406685.1612149640',
                'User-Agent': ua.random
            }
            cur_url = self.base_url.replace('-', str(film_id))
            response = requests.get(url=cur_url, headers=headers)
            if response.status_code is not 200:
                return 1
            return response.text

    def get_film_info(self, html_doc, film_id):
        bs = BeautifulSoup(html_doc, 'html.parser')
        year_object = bs.find(class_='year')
        year = ''
        if year_object is not None:
            search_result = re.search(r'\d+', year_object.text)
            if search_result is not None:
                year = search_result.group(0)

        info = bs.find(id='info')
        if info is not None:
            genre = ''
            genre_list = info.find_all(attrs={"property": "v:genre"})
            if genre_list is not None:
                for item in genre_list:
                    genre += item.text + ' '
            genre = genre.strip()

            release_date = ''
            release_list = info.find_all(attrs={"property": "v: initialReleaseDate"})
            if release_list is not None:
                for item in release_list:
                    release_str = release_list.text
                    if '中国大陆' in release_str or '中国台湾' in release_str or '中国香港' in release_str:
                        search_result = re.search(r'\d\d\d\d-\d\d-\d\d', release_str)
                        if search_result is not None:
                            release_date = search_result.group(0)

            runtime = ''
            runtime_object = info.find_all(attrs={"property": "v:runtime"})
            if runtime_object is not None and runtime_object.__len__() > 0:
                runtime_str = runtime_object.text
                search_result = re.search(r'\d+', runtime_str)
                if search_result is not None:
                    runtime = search_result.group(0)

            span_p1 = bs.find_all(class_='pl')
            short_comment_num = ''
            film_comment_num = ''
            for item in span_p1:
                href = 'https://movie.douban.com/subject/%s/comments?status=P' % film_id

                short_comment_object = item.find(attrs={"href": href})
                if short_comment_object is not None:
                    short_str = short_comment_object.text
                    search_result = re.search(r'\d+', short_str)
                    if search_result is not None:
                        short_comment_num = search_result.group(0)
                        continue

                film_comment_object = item.find(attrs={"href": "reviews"})
                if film_comment_object is not None:
                    film_comment_str = film_comment_object.text
                    search_result = re.search(r'\d+', film_comment_str)
                    if search_result is not None:
                        film_comment_num = search_result.group(0)

            dict_film_info = {}
            dict_film_info['year'] = year
            dict_film_info['genre'] = genre
            dict_film_info['release_date'] = release_date
            dict_film_info['runtime'] = runtime
            dict_film_info['short_comment_num'] = short_comment_num
            dict_film_info['film_comment_num'] = film_comment_num

            return dict_film_info

    def write_db(self, dict_film_info, film_id):
        sql = 'select * from douban_film_info where filmId=%d' % film_id
        film_info = ''
        if self.operator.search(sql).__len__() > 0:
            film_info = '已存在'
        else:
            sql = 'update douban_film_info set year="%s", genre="%s", releaseDate="%s", runtime="%s", shortComment="%s", filmComment="%s"' \
                  'where filmId=%d' % \
                  (dict_film_info['year'], dict_film_info['genre'], dict_film_info['release_date'], dict_film_info['runtime'],
                   dict_film_info['short_comment'], dict_film_info['film_comment'], film_id)
            if self.operator.execute_sql(sql) == 0:
                film_info = '成功'
            else:
                film_info = '失败'
        print('%s%s' % (film_id, film_info))

    def read_db(self):
        self.operator.conn_mysql()
        sql = 'select filmId from douban_film_info'
        result = self.operator.search(sql)
        if result == 1:
            print('从数据库读取影片Id出错')
        return result


def official_method():
    crawler = DoubanFilmInfoCrawler()
    getor = get_url.GetUrl()
    film_id_list = crawler.read_db()
    cfo = config_operator.ConfigOperator()
    offset = int(cfo.get_douban_film('filminfo_offset'))
    interval = int(cfo.get_douban_film('filminfo_interval'))
    for num in range(offset, film_id_list.__len__()):
        film_id = int(film_id_list[num][0])
        url = crawler.base_url.replace('-', str(film_id))
        try:
            response = getor.get_response(url)
            dict_film_info = crawler.get_film_info(response.text, film_id)
            crawler.write_db(dict_film_info, film_id)
            cfo.write_maoyan_film('filminfo_offset', num.__str__())
        except Exception as e:
            while 1:
                try:
                    print('出现异常，30s后重试\n' + str(e))
                    getor.change_account()
                    time.sleep(30)
                    response = getor.get_response(url)
                    dict_film_info = crawler.get_film_info(response.text, film_id)
                    crawler.write_db(dict_film_info, film_id)
                    cfo.write_maoyan_film('filminfo_offset', num.__str__())
                    break
                except:
                    pass

        time.sleep(interval)


if __name__ == '__main__':
    official_method()
