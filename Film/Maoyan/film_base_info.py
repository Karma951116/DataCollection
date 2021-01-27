# Module For Data In Page https://maoyan.com/films/
# Fields : year, film_name, box_office, release_date, country, genre,award_name, award_detail1, award_detail2
# director, director_id, actor, actor_id, actor_role

import requests
import os
import re
import time

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from Utils import mysql_operator
from Utils import config_operator


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
        if brief_container is None:
            return
        film_name_object = brief_container.find('h1')
        film_ename_object = brief_container.find(class_='ename ellipsis')
        if film_name_object is None:
            return
        film_name = film_name_object.string
        film_ename = ''
        if film_ename_object is not None:
            film_ename = film_ename_object.string
            if film_ename is None:
                film_ename = ''
        # 获取三个li标签
        li_list = brief_container.find_all('li')
        # 获取带有影片类型的a标签
        # li_0 影片类型
        a_list = li_list[0].find_all('a')
        genre = ''
        for a in a_list:
            genre += a.string + ','
        genre = genre.replace(',', '', -1).strip()
        # li_1 制片地区、时长
        run_time = ''
        product_country = ''
        if li_list[1] is not None and li_list[1].string is not None:
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
        if li_list[2] is not None:
            # year
            if li_list[2].string is not None:
                search_result = re.search(r'\d\d\d\d', li_list[2].string)
                if search_result is not None:
                    year = search_result.group(0)
                # release_date
                search_result = re.search(r'\d+-\d+-\d+', li_list[2].string)
                if search_result is not None:
                    release_date = search_result.group(0)
                # show_country
                search_result = re.search(r'[\u4E00-\u9FA5\s]+', li_list[2].string)
                if search_result is not None:
                    show_country = search_result.group(0)
                    show_country = show_country.replace('上映', '')

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
            if img_url is not None or img_url != '':
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
        if cast_staff_module is None:
            return

        h2_object = cast_staff_module.find('h2')
        if h2_object is None:
            return

        h2 = h2_object.string
        if h2 == '演职人员':
            celebrity_group = cast_staff_module.find_all(class_='celebrity-group')
            for group in celebrity_group:
                # 获取人员类型，director/actor
                celebrity_type_zh_object = group.find(class_='celebrity-type')
                celebrity_type_zh = ''
                if celebrity_type_zh_object is not None:
                    celebrity_type_zh = celebrity_type_zh_object.string.strip()

                li_list = group.find_all('li')
                for li in li_list:
                    celebrity_id = re.search(r'\d+', li.get('data-val')).group(0).strip()
                    celebrity_name = li.find(class_='name').string.strip()
                    role = ''
                    role_object = li.find(class_='role')

                    if role_object is not None:
                        role = role_object.string
                        role = role.replace('饰：', '').strip()
                    staff_info = []
                    staff_info.append(celebrity_type_zh)
                    staff_info.append(celebrity_name)
                    staff_info.append(role)
                    dict_cast_staff[celebrity_id] = staff_info

        return dict_cast_staff

    def get_award_info(self, html_doc):
        bs = BeautifulSoup(html_doc, 'html.parser')
        div_award_tab = bs.find(class_='tab-award tab-content')
        if div_award_tab is None:
            return

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
                if '提名' in div.string:
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
        # 影片基本信息查重
        sql = 'select * from maoyan_film_baseinfo where filmId = %d' % film_id
        if self.operator.search(sql).__len__() > 0:
            base_info = '已存在'
        else:
            sql = 'insert into maoyan_film_baseinfo (filmId, filmName, year, releaseDate, runtime, showCountry, genre, productCountry, filmEngName) ' \
                  'values(%d, "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")' % \
                  (film_id, dict_base_info['film_name'], dict_base_info['year'], dict_base_info['release_date'],
                   dict_base_info['run_time'], dict_base_info['show_country'], dict_base_info['genre'],
                   dict_base_info['product_country'], dict_base_info['film_ename'])

            if self.operator.execute_sql(sql) == 0:
                base_info = '成功'
            else:
                base_info = '失败'

        staff_num = len(dict_cast_staff)
        staff_already = 0
        staff_succ = 0
        staff_failed = 0
        if len(dict_cast_staff) <= 0:
            staff = '无信息'
        for key in dict_cast_staff:
            info_list = dict_cast_staff[key]
            # 影片演员查重 infolist[0]:type, infolist[1]:name, infolist[2]:role, key:celebrityId
            sql = 'select * from maoyan_film_celebrity where filmId=%d and celebrityId=%d and role = "%s"' \
                  % (film_id, int(key), info_list[2])
            if self.operator.search(sql).__len__() > 0:
                staff_already += 1
            else:
                sql = 'insert into maoyan_film_celebrity (filmId, type, name, celebrityId, role)' \
                      'values(%d, "%s", "%s", %d, "%s")' % (film_id, info_list[0], info_list[1], int(key), info_list[2])
                if self.operator.execute_sql(sql) == 0:
                    staff_succ += 1
                else:
                    staff_failed += 1
        staff = '[数量: %d,已存在: %d, 成功: %d, 失败: %d]' % \
                (int(staff_num), int(staff_already), int(staff_succ), int(staff_failed))

        awards_num = len(dict_awards)
        awards_already = 0
        awards_succ = 0
        awards_failed = 0
        if len(dict_awards) <= 0:
            awards = '无信息'
        for key in dict_awards:
            info_list = dict_awards[key]
            # 影片获奖查重 key:大奖名称,info_list[0]:获奖,info_list[1]:提名
            sql = 'select * from maoyan_film_awards where filmId = %d and portrait = "%s"' \
                  % (film_id, key)
            if self.operator.search(sql).__len__() > 0:
                awards_already += 1
            else:
                sql = 'insert into maoyan_film_awards (filmId, portrait, award, nominate) ' \
                      'values(%d, "%s", "%s", "%s")' % (film_id, key, info_list[0], info_list[1])
                if self.operator.execute_sql(sql) == 0:
                    awards_succ += 1
                else:
                    awards_failed += 1
        awards = '[数量:%d, 已存在: %d, 成功: %d, 失败: %d]' % \
                 (int(awards_num), int(awards_already), int(awards_succ), int(awards_failed))
        print('%s;%d;base_info%s;staff%s;awards%s' % (dict_base_info['film_name'], film_id, base_info, staff, awards))

def official_method():
    crawler = FilmBaseInfoCrawler()
    film_id_list = crawler.read_film_id()
    cfo = config_operator.ConfigOperator()
    offset = int(cfo.get_film_baseinfo('current_offset'))
    for num in range(offset, film_id_list.__len__()):
        film_id = int(film_id_list[num][0])
        html_doc = crawler.get_response(film_id)
        dict_base_info = crawler.get_base_info(html_doc, film_id)
        dict_cast_staff = crawler.get_staff_info(html_doc)
        dict_awards = crawler.get_award_info(html_doc)
        crawler.write_db(dict_base_info, dict_cast_staff, dict_awards, film_id)
        cfo.write_film_baseinfo('current_offset', num.__str__())
        time.sleep(25)


if __name__ == '__main__':
    official_method()
