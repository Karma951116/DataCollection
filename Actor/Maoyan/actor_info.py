# 获取猫眼影人的所有信息
import requests
import json
import re
import time

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from Utils import mysql_operator
from Utils import config_operator


class CelebrityInfoCrawler:
    def __init__(self):
        self.base_url = 'https://maoyan.com/films/celebrity/-'
        self.operator = mysql_operator.MysqlOperator()

    def get_response(self, celebrity_id):
        # 伪装请求
        ua = UserAgent()
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
            'Connection': 'keep-alive',
            'Cookie': '__mta=217831081.1608186388084.1611832389971.1611832764462.110; _lxsdk_cuid=1766f5f7d67c8-0624e350695b8c-c791039-1fa400-1766f5f7d67c8; recentCis=1%3D151%3D140%3D84%3D197; theme=moviepro; _bsin_180503_=[1,10,11,12,13,14,15,16,17,2,3,4,5,6,7,8,9]; _lxsdk=7B68F3905C8A11EBB5F151AF051690DE1D7402F0C99D456A8D6E3BB78EA2E993; Hm_lvt_703e94591e87be68cc8da0da7cbd0be2=1611282588,1611303543,1611303548,1611303555; __mta=217831081.1608186388084.1611817639496.1611819045156.107; __mta=217831081.1608186388084.1611819045156.1611828685079.108; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; Hm_lpvt_703e94591e87be68cc8da0da7cbd0be2=1611896259; _lxsdk_s=1774e69f736-905-b9a-b49%7C1097461883%7C1',
            'User-Agent': ua.random
        }
        cur_url = self.base_url.replace('-',  celebrity_id.__str__())
        response = requests.get(url=cur_url, headers=headers)
        if response.status_code is not 200:
            return 1
        return response.text

    def read_celebrity_id(self):
        self.operator.conn_mysql()
        sql = 'select celebrityId from maoyan_film_celebrity'
        result = self.operator.search(sql)
        if result == 1:
            print('从数据库读取影片Id出错')
        return result

    def get_celebrity_info(self, bs):
        name_object = bs.find(class_='china-name cele-name')
        name = ''
        if name_object is not None:
            name = name_object.text

        ename_object = bs.find(class_='eng-name cele-name')
        ename = ''
        if name_object is not None:
            ename = ename_object.text

        cele_desc = bs.find(class_='cele-desc')
        introduce = ''
        if cele_desc is not None:
            introduce = cele_desc.text

        dl_left = bs.find(class_='dl-left')
        dl_right = bs.find(class_='dl-right')

        dict_info = {}
        dict_info['birthday'] = ''
        dict_info['born'] = ''
        dict_info['sex'] = ''
        dict_info['jobs'] = ''
        dict_info['nationality'] = ''

        if dl_left is not None:
            title_list = dl_left.find_all(class_='basicInfo-item name')
            value_list = dl_left.find_all(class_='basicInfo-item value')
            if title_list is not None and value_list is not None:
                for num in range(title_list.__len__()):
                    title = title_list[num].text
                    title = ''.join(title.split())
                    value = value_list[num].text.strip()
                    if title == '出生日期':
                        dict_info['birthday'] = value
                    if title == '出生地':
                        dict_info['born'] = value
                    if title == '性别':
                        dict_info['sex'] = value
                    if title == '身份':
                        dict_info['jobs'] = value
                    if title == '国籍':
                        dict_info['nationality'] = value

        if dl_right is not None:
            title_list = dl_right.find_all(class_='basicInfo-item name')
            value_list = dl_right.find_all(class_='basicInfo-item value')
            if title_list is not None and value_list is not None:
                for num in range(title_list.__len__()):
                    title = title_list[num].text
                    title = ''.join(title.split())
                    value = value_list[num].text.strip()
                    if title == '出生日期':
                        dict_info['birthday'] = value
                    if title == '出生地':
                        dict_info['born'] = value
                    if title == '性别':
                        dict_info['sex'] = value
                    if title == '身份':
                        dict_info['jobs'] = value
                    if title == '国籍':
                        dict_info['nationality'] = value

        dict_info['name'] = name
        dict_info['ename'] = ename
        dict_info['introduce'] = introduce

        return dict_info

    def get_awards(self, bs):
        award_block = bs.find(class_='award')
        if award_block is None:
            dict = {}
            return dict

        award_name_block = award_block.find(class_='mod-content')
        if award_name_block is None:
            dict = {}
            return None

        award_name_list = award_name_block.find_all(class_='item')

        award_detail_block = bs.find(class_='award-detail')
        if award_detail_block is None:
            dict = {}
            return None
        # 获奖信息
        dict_awards = {}
        for item in award_name_list:
            # list包含该大奖下所有获奖详情
            list_award_infos = []
            # 获取大奖名称
            num = item.get('data-index')
            if num is not None:
                num = int(num)
            name_object = item.find(class_='award-name')
            if name_object is None:
                continue
            name = name_object.text
            # 根据奖项的data-index属性找到对应获奖详情
            detail_item = award_detail_block.find(attrs={"data-index": str(num)})
            list_li = None
            if detail_item is not None:
                list_li = detail_item.find_all('li')
            if list_li is not None:
                for li in list_li:
                    dict_award_info = {}
                    # list顺序：奖项，获奖影片，年份，饰演角色
                    detail_left = li.find(class_='detail-left')
                    award = ''
                    if detail_left is not None:
                        award = detail_left.text

                    detail_right = li.find(class_='detail-right')
                    if detail_right is not None and detail_left.text != '':
                        a_movie_name = detail_right.find('a')
                        movie_name = ''
                        if a_movie_name is not None:
                            movie_name = a_movie_name.text
                            movie_name = movie_name.replace('《', '')
                            movie_name = movie_name.replace('》', '')

                        year = ''
                        role = ''
                        info_str = detail_right.text
                        if info_str is not None and info_str != '':
                            info_str = info_str.strip()
                            search_result = re.search(r'\d\d\d\d', info_str)
                            if search_result is not None:
                                year = search_result.group(0)
                            search_result = re.search(r'饰:[\u4E00-\u9FA5\s]+', info_str)
                            if search_result is not None:
                                role = search_result.group(0)
                                role = role.replace('饰:', '').strip()
                        dict_award_info['award'] = award
                        dict_award_info['movie'] = movie_name
                        dict_award_info['year'] = year
                        dict_award_info['role'] = role
                    list_award_infos.append(dict_award_info)

            dict_awards[name] = list_award_infos
        return dict_awards

    def get_coactors(self, bs):
        relate_list = bs.find_all(class_='rel-item')
        if relate_list is None:
            list = []
            return list
        list_coactors = []
        for item in relate_list:
            a_id = item.find('a')
            if a_id is None:
                continue
            id_str = a_id.get('data-val')
            coactor_id = None
            if id_str is not None and id_str != '':
                search_result = re.search(r'\d+', id_str)
                if search_result is not None:
                    coactor_id = int(search_result.group(0))
            if coactor_id is None:
                continue
            p_name = item.find(class_='rel-name')
            name = ''
            if p_name is not None:
                name = p_name.text
            p_relation = item.find(class_='rel-relation')
            relation = ''
            if p_relation is not None:
                relation = p_relation.text

            dict_coactor = {}
            dict_coactor['coactor_id'] = coactor_id
            dict_coactor['coactor_name'] = name
            dict_coactor['coactor_relation'] = relation
            list_coactors.append(dict_coactor)

        return list_coactors

    def write_db(self, dict_info, dict_awards, list_coactors, celebrity_id):
        # 影人基本信息
        base_info = ''
        sql = 'select * from maoyan_celebrity_baseinfo where celebrityId=%d' % celebrity_id
        if self.operator.search(sql).__len__() > 0:
            sql = 'update maoyan_celebrity_baseinfo set name="%s", ename="%s", sex="%s", jobs="%s", born="%s", ' \
                  'birthday="%s", introduce="%s", nationality="%s" where celebrityId=%d' % \
                  (dict_info['name'], dict_info['ename'], dict_info['sex'], dict_info['jobs'], dict_info['born'],
                   dict_info['birthday'], dict_info['introduce'], dict_info['nationality'], celebrity_id)
            if self.operator.execute_sql(sql) == 0:
                base_info = '已更新'
            else:
                base_info = '更新失败'
        else:
            sql = 'insert into maoyan_celebrity_baseinfo (celebrityId, name, ename, sex, jobs, born, birthday, introduce, nationality) ' \
                  'values(%d, "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")' % \
                  (celebrity_id, dict_info['name'], dict_info['ename'], dict_info['sex'], dict_info['jobs'], dict_info['born'],
                    dict_info['birthday'], dict_info['introduce'], dict_info['nationality'])
            if self.operator.execute_sql(sql) == 0:
                base_info = '成功'
            else:
                base_info = '失败'

        # 影人获奖信息
        award_num = 0
        award_already = 0
        award_succ = 0
        award_failed = 0
        for key in dict_awards:
            list_details = dict_awards[key]
            for dict in list_details:
                award_num += 1
                sql = 'select * from maoyan_celebrity_awards where celebrityId=%d and awardName="%s" and awardDetail="%s"' % \
                      (celebrity_id, str(key), dict['award'])
                if self.operator.search(sql).__len__() > 0:
                    award_already += 1
                else:
                    sql = 'insert into maoyan_celebrity_awards (celebrityId, awardName, awardDetail, awardFilm, awardYear, awardRole) ' \
                          'values(%d, "%s", "%s", "%s", "%s", "%s")' % \
                          (celebrity_id, str(key), dict['award'], dict['movie'], dict['year'], dict['role'])
                    if self.operator.execute_sql(sql) == 0:
                        award_succ += 1
                    else:
                        award_failed += 1
        award_str = '[数量：%d, 已更新：%d，成功：%d， 失败：%d]' % (award_num, award_already, award_succ, award_failed)
        # 相关影人
        relative_num = len(list_coactors)
        relative_update = 0
        relative_succ = 0
        relative_failed = 0
        for dict in list_coactors:
            sql = 'select * from maoyan_celebrity_coactors where celebrityId=%d and coActorId=%d' % \
                  (celebrity_id, dict['coactor_id'])
            if self.operator.search(sql).__len__() > 0:
                sql = 'update maoyan_celebrity_coactors set relation = "%s"' % dict['coactor_relation']
                if self.operator.execute_sql(sql) == 0:
                    relative_update += 1
                else:
                    relative_failed += 1
            else:
                sql = 'insert into maoyan_celebrity_coactors (celebrityId, coActorId, relation) values(%d, %d, "%s")' % \
                      (celebrity_id, dict['coactor_id'], dict['coactor_relation'])
                if self.operator.execute_sql(sql) == 0:
                    relative_succ += 1
                else:
                    relative_failed += 1
        relative_str = '[数量：%d, 已更新：%d，成功：%d， 失败：%d]' % (relative_num, relative_update, relative_succ, relative_failed)
        print('%d;baseinfo%s;awards%s;relative%s' % (celebrity_id, base_info, award_str, relative_str))


def official_method():
    crawler = CelebrityInfoCrawler()
    celebrity_id_list = crawler.read_celebrity_id()
    cfo = config_operator.ConfigOperator()
    offset = int(cfo.get_maoyan_film('actor_offset'))
    interval = int(cfo.get_maoyan_film('actor_interval'))
    for num in range(offset, celebrity_id_list.__len__()):
        celebrity_id = int(celebrity_id_list[num][0])
        html_doc = crawler.get_response(celebrity_id)
        bs = BeautifulSoup(html_doc, 'html.parser')
        dict_info = crawler.get_celebrity_info(bs)
        dict_awards = crawler.get_awards(bs)
        list_coactors = crawler.get_coactors(bs)
        crawler.write_db(dict_info, dict_awards, list_coactors, celebrity_id)
        cfo.write_maoyan_film('actor_offset', num.__str__())
        time.sleep(interval)


def single_method(celebrity_id):
    crawler = CelebrityInfoCrawler()
    html_doc = crawler.get_response(celebrity_id)
    bs = BeautifulSoup(html_doc, 'html.parser')
    dict_info = crawler.get_celebrity_info(bs)
    dict_awards = crawler.get_awards(bs)
    list_coactors = crawler.get_coactors(bs)
    crawler.write_db(dict_info, dict_awards, list_coactors, celebrity_id)
    crawler.operator.close_mysql()


if __name__ == '__main__':
    official_method()