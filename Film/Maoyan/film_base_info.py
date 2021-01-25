# Module For Data In Page https://maoyan.com/films/
# Fields : year, film_name, box_office, release_date, country, genre,award_name, award_detail1, award_detail2
# director, director_id, actor, actor_id, actor_role

import requests
import os
import re
import time
import urllib

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from Utils import mysql_operator

class FilmBaseInfoCrawler:
    def __init__(self):
        self.base_url = 'https://maoyan.com/films/'
        self.poster_path = os.path.dirname(os.path.abspath('..')) + '\\Resources\\MaoyanFilmPosters\\'
        self.operator = mysql_operator.MysqlOperator()

    def get_response(self, film_id):
        # 伪装请求
        ua = UserAgent()
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;'
                      'q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
            'Connection': 'keep-alive',
            'Cookie': '__mta=217831081.1608186388084.1611555167551.1611555192384.85;'
                      ' _lxsdk_cuid=1766f5f7d67c8-0624e350695b8c-c791039-1fa400-1766f5f7d67c8;'
                      ' uuid_n_v=v1; recentCis=1%3D151%3D140%3D84%3D197;'
                      ' theme=moviepro; uuid=7B68F3905C8A11EBB5F151AF051690DE1D7402F0C99D456A8D6E3BB78EA2E993;'
                      ' _csrf=39b55a714339fd6eb8dd2bdf8b9601b85995049444885b7d2fca31e09257b05d;'
                      ' lt=t8I8mcKnX404DBn7MDrMn9T2PywAAAAAhgwAAIJf-Hmi1-ckw9XwgXMbD0YhmxgCBjMekOUcsBEaEcjR2Nt1V9nIMxa9JpQFElsHGA;'
                      ' lt.sig=t6O-xJMugn98A3qEGGWp7tdK3js; uid=1097461883; uid.sig=Qzn0f6oHqGvrFPGYvWuz1uoqSK0;'
                      ' _lxsdk=7B68F3905C8A11EBB5F151AF051690DE1D7402F0C99D456A8D6E3BB78EA2E993;'
                      ' Hm_lvt_703e94591e87be68cc8da0da7cbd0be2=1611282588,1611303543,1611303548,1611303555;'
                      ' _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic;'
                      ' __mta=217831081.1608186388084.1611551400561.1611551638864.78;'
                      ' Hm_lpvt_703e94591e87be68cc8da0da7cbd0be2=1611555192;'
                      ' _lxsdk_s=17738867c60-e6d-6b8-806%7C1097461883%7C1',
            'User-Agent': ua.random
        }
        cur_url = self.base_url + film_id.__str__() + '#award'
        response = requests.get(url=cur_url, headers=headers)
        if response.status_code is not 200:
            return 1
        return response.text

    def get_base_info(self, html_doc, film_id):
        # 这里一定要指定解析器，可以使用默认的html，也可以使用lxml比较快。
        bs = BeautifulSoup(html_doc, 'html.parser')
        # print(soup.prettify());channel-detail movie-item-title
        # 获取右侧简介区域
        brief_container = bs.find(class_='movie-brief-container')
        film_name = brief_container.find('h1').string
        film_ename = brief_container.find(class_='ename ellipsis').string
        # 获取三个li标签
        li_list = brief_container.find_all('li')
        # 获取带有影片类型的a标签
        # li_0 影片类型
        a_list = li_list[0].find_all('a')
        genre = ''
        for a in a_list:
            genre += a.string + ','
        genre.replace(',', '', -1)
        # li_1 制片地区、时长
        run_time = ''
        product_country = ''
        str_list = li_list[1].string.split('/')
        for str_item in str_list:
            search_result = re.search(r'\d+', str_item.strip())
            if search_result is not None:
                run_time = search_result.group(0)
            else:
                product_country = str_item.strip()
        # li_2 上映时间、上映地区、年份
        release_date = ''
        show_country = ''
        year = ''

        search_result = re.search(r'\d+-\d+-\d+', li_list[2].string)
        if search_result is not None:
            release_date = search_result.group(0)
            year = re.search(r'\d\d\d\d', release_date).group(0)

        search_result = re.search(r'[\u4E00-\u9FA5\s]+', li_list[2].string)
        if search_result is not None:
            show_country = search_result.group(0)
        print(film_name, film_ename, genre, product_country, run_time, release_date, show_country)
        dict_base_info = {'film_name': film_name}
        dict_base_info['film_ename'] = film_ename
        dict_base_info['product_country'] = product_country
        dict_base_info['run_time'] = run_time
        dict_base_info['release_date'] = release_date
        dict_base_info['show_country'] = show_country
        dict_base_info['genre'] = genre
        dict_base_info['year'] = year
        # 下载影片海报
        poster = bs.find(class_='celeInfo-left')
        if poster is not None:
            img_url = poster.find('img').get('src')
            poster_response = self.get_poster(img_url)
            if poster_response is not None:
                open(self.poster_path + '%d' % film_id, 'wb').write(poster_response.content)

        return dict_base_info

    def get_poster(self, img_url):
        # 伪装请求
        ua = UserAgent()
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;'
                      'q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
            'Connection': 'keep-alive',
            'Cookie': '__mta=217831081.1608186388084.1611538197495.1611538219962.69;'
                      ' _lxsdk_cuid=1766f5f7d67c8-0624e350695b8c-c791039-1fa400-1766f5f7d67c8;'
                      ' uuid_n_v=v1; recentCis=1%3D151%3D140%3D84%3D197; theme=moviepro;'
                      ' _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic;'
                      ' uuid=7B68F3905C8A11EBB5F151AF051690DE1D7402F0C99D456A8D6E3BB78EA2E993;'
                      ' _csrf=39b55a714339fd6eb8dd2bdf8b9601b85995049444885b7d2fca31e09257b05d;'
                      ' lt=t8I8mcKnX404DBn7MDrMn9T2PywAAAAAhgwAAIJf-Hmi1-ckw9XwgXMbD0YhmxgCBjMekOUcsBEaEcjR2Nt1V9nIMxa9JpQFElsHGA;'
                      ' lt.sig=t6O-xJMugn98A3qEGGWp7tdK3js; uid=1097461883; uid.sig=Qzn0f6oHqGvrFPGYvWuz1uoqSK0;'
                      ' _lxsdk=7B68F3905C8A11EBB5F151AF051690DE1D7402F0C99D456A8D6E3BB78EA2E993;'
                      ' Hm_lvt_703e94591e87be68cc8da0da7cbd0be2=1611282588,1611303543,1611303548,1611303555;'
                      ' Hm_lpvt_703e94591e87be68cc8da0da7cbd0be2=1611540685;'
                      ' __mta=217831081.1608186388084.1611538219962.1611540685904.70;'
                      ' _lxsdk_s=17736fd8991-189-61f-de3%7C1097461883%7C108',
            'User-Agent': ua.random
        }
        response = requests.get(url=img_url, headers=headers)
        if response.status_code is not 200:
            return 1
        return response

    def get_staff_info(self, html_doc):
        bs = BeautifulSoup(html_doc, 'html.parser')
        # 获取演员信息
        dict_cast_staff = {}
        cast_staff_module = bs.find(class_='tab-desc tab-content active').find_all(class_='module')[1]
        h2 = cast_staff_module.find('h2').string
        if h2 == '演职人员':
            celebrity_group = cast_staff_module.find_all(class_='celebrity-group')
            for group in celebrity_group:
                # 获取人员类型，director/actor
                celebrity_type_zh = group.find(class_='celebrity-type').string.strip()
                celebrity_type_eng = ''
                if celebrity_type_zh == '导演':
                    celebrity_type_eng = 'director'
                else:
                    celebrity_type_eng = 'actor'

                li_list = group.find_all('li')
                for li in li_list:
                    celebrity_id = re.search(r'\d+', li.get('data-val')).group(0).strip()
                    celebrity_name = li.find(class_='name').string.strip()
                    role = li.find(class_='role').string.strip()
                    staff_info = []
                    staff_info.append(celebrity_type_eng)
                    staff_info.append(celebrity_name)
                    staff_info.append(role)
                    dict_cast_staff[celebrity_id] = staff_info

        return dict_cast_staff

    def get_award_info(self, html_doc):
        bs = BeautifulSoup(html_doc, 'html.parser')
        div_award_tab = bs.find(class_='tab-award tab-content')
        li_list = div_award_tab.find_all('li')
        dict_awards = {}
        for li in li_list:
            portrait = li.find('div').text.strip()
            content = li.find(class_='content')
            div_list = content.find_all('div')
            award = ''
            nominate = ''
            for div in div_list:
                if '获奖' in div.string:
                    award = div.string.replace('获奖：', '').strip()
                else:
                    nominate = div.string.replace('提名：', '').strip()
            list_temp = [award, nominate]
            dict_awards[portrait] = list_temp

        return dict_awards

    def read_film_id(self):
        self.operator.conn_mysql()
        sql = 'select filmId from maoyan_filmid'
        result = self.operator.search(sql)
        if result == 1:
            print('从数据库读取影片Id出错')
        return result

    def write_db(self, dict_base_info, dict_cast_staff, dict_awards, film_id):
        base_info = ''
        staff = ''
        awards = ''
        # 影片基本信息查重
        sql = 'select * from maoyan_filmid where filmId = "%d"' % film_id
        if self.operator.search(sql).__len__() > 0:
            base_info = '已存在'
        else:
            sql = 'insert into maoyan_film_baseinfo (filmId, filmName, year, releaseDate, runtime, showCountry, genre, productCountry, filmEngName) ' \
                  'values(%d, %s, %s, %s, %s, %s, %s, %s, %s)' % \
                  (film_id, dict_base_info['film_name'], dict_base_info['year'], dict_base_info['release_date'],
                   dict_base_info['run_time'], dict_base_info['show_country'], dict_base_info['genre'],
                   dict_base_info['product_country'], dict_base_info['film_ename'])

            if self.operator.execute_sql(sql) == 0:
                base_info = '成功'
            else:
                base_info = '失败'

        #for key in dict_cast_staff:
            #sql = 'insert into maoyan_film_celebrity (filmId, type, name, celebrityId, role)'


def official_method():
    crawler = FilmBaseInfoCrawler()
    film_id_list = crawler.read_film_id()
    for film_id_tuple in film_id_list:
        film_id = film_id_tuple[0]
        html_doc = crawler.get_response(film_id)
        dict_base_info = crawler.get_base_info(html_doc, film_id)
        dict_cast_staff = crawler.get_staff_info(html_doc)
        dict_awards = crawler.get_award_info(html_doc)


if __name__ == '__main__':
    official_method()
