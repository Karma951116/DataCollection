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


class BaiduIndexActorAgeGenderCrawler:
    def __init__(self):
        self.base_url = 'http://index.baidu.com/api/SocialApi/baseAttributes?wordlist[]='
        self.operator = mysql_operator.MysqlOperator()

    def get_info(self, html_doc):
        dict_out = json.loads(html_doc)
        dict_data = dict_out['data']
        list_result = dict_data['result']
        index_result = list_result[0]
        allnet_result = list_result[1]

        # gender
        list_index_gender = index_result['gender']
        dict_gender_return = {
            'male': '',
            'male_tgi': '',
            'female': '',
            'female_tgi': '',
            'male_all_net': '',
            'female_all_net': ''
        }
        for item in list_index_gender:
            if item['desc'] == '男':
                dict_gender_return['male'] = item['rate']
                dict_gender_return['male_tgi'] = item['tgi']
            else:
                dict_gender_return['female'] = item['rate']
                dict_gender_return['female_tgi'] = item['tgi']

        list_allnet_gender = allnet_result['gender']
        for item in list_allnet_gender:
            if item['desc'] == '男':
                dict_gender_return['male_all_net'] = item['rate']
            else:
                dict_gender_return['female_all_net'] = item['rate']

        # age
        list_index_age = index_result['age']
        dict_age_return = {
            '0-19': '',
            '0-19_tgi': '',
            '20-29': '',
            '20-29_tgi': '',
            '30-39': '',
            '30-39_tgi': '',
            '40-49': '',
            '40-49_tgi': '',
            '50+': '',
            '50+_tgi': '',
            '0-19_all_net': '',
            '20-29_all_net': '',
            '30-39_all_net': '',
            '40-49_all_net': '',
            '50+_all_net': ''
        }
        for item in list_index_age:
            if item['desc'] == '0-19':
                dict_age_return['0-19'] = item['rate']
                dict_age_return['0-19_tgi'] = item['tgi']
            elif item['desc'] == '20-29':
                dict_age_return['20-29'] = item['rate']
                dict_age_return['20-29_tgi'] = item['tgi']
            elif item['desc'] == '30-39':
                dict_age_return['30-39'] = item['rate']
                dict_age_return['30-39_tgi'] = item['tgi']
            elif item['desc'] == '40-49':
                dict_age_return['40-49'] = item['rate']
                dict_age_return['40-49_tgi'] = item['tgi']
            elif item['desc'] == '50+':
                dict_age_return['50+'] = item['rate']
                dict_age_return['50+_tgi'] = item['tgi']

        list_allnet_age = allnet_result['age']
        for item in list_allnet_age:
            if item['desc'] == '0-19':
                dict_age_return['0-19_all_net'] = item['rate']
            elif item['desc'] == '20-29':
                dict_age_return['20-29_all_net'] = item['rate']
            elif item['desc'] == '30-39':
                dict_age_return['30-39_all_net'] = item['rate']
            elif item['desc'] == '40-49':
                dict_age_return['40-49_all_net'] = item['rate']
            elif item['desc'] == '50+':
                dict_age_return['50+_all_net'] = item['rate']


        dict_result = {}
        dict_result['gender'] = dict_gender_return
        dict_result['age'] = dict_age_return

        return dict_result

    def write_db(self, dict_result, celebrity_id):
        dict_gender = dict_result['gender']
        dict_age = dict_result['age']

        # gender
        gender = ''
        sql = 'select * from baiduindex_celebrity_gender where celebrityId=%d' % celebrity_id
        if self.operator.search(sql).__len__() > 0:
            gender = '已存在'
        else:
            sql = 'insert into baiduindex_celebrity_gender (celebrityId, male, female, maleTgi, femaleTgi, maleAllNet, femaleAllNet)' \
                  'values(%d, "%s", "%s", "%s", "%s", "%s", "%s")' % \
                  (celebrity_id, dict_gender['male'], dict_gender['female'], dict_gender['male_tgi'], dict_gender['female_tgi'],
                   dict_gender['male_all_net'], dict_gender['female_all_net'])
            if self.operator.execute_sql(sql) == 0:
                gender = '成功'
            else:
                gender = '失败'

        # age
        age = ''
        sql = 'select * from baiduindex_celebrity_age where celebrityId=%d' % celebrity_id
        if self.operator.search(sql).__len__() > 0:
            age = '已存在'
        else:
            sql = 'insert into baiduindex_celebrity_age (celebrityId, under19Index, under19AllNet, under19Tgi, 20to29Index, 20to29AllNet, 20to29Tgi,' \
                  '30to39Index, 30to39AllNet, 30to39Tgi, 40to49Index, 40to49AllNet, 40to49Tgi, above50Index, above50AllNet, above50Tgi) values' \
                  '(%d, "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")' % \
                  (celebrity_id, dict_age['0-19'], dict_age['0-19_all_net'], dict_age['0-19_tgi'],
                   dict_age['20-29'], dict_age['20-29_all_net'], dict_age['20-29_tgi'],
                   dict_age['30-39'], dict_age['30-39_all_net'], dict_age['30-39_tgi'],
                   dict_age['40-49'], dict_age['40-49_all_net'], dict_age['40-49_tgi'],
                   dict_age['50+'], dict_age['50+_all_net'], dict_age['50+_tgi'])
            if self.operator.execute_sql(sql) == 0:
                age = '成功'
            else:
                age = '失败'
        print('%d;gender%s;age%s' % (celebrity_id, gender, age))

    def read_db_name(self):
        self.operator.conn_mysql()
        sql = 'select celebrityId,name from maoyan_celebrity_baseinfo'
        result = self.operator.search(sql)
        if result == 1:
            print('从数据库读取影片Id出错')
        return result

    def check_response_available(self, html_doc):
        dict_out = json.loads(html_doc)
        if dict_out['status'] is None or dict_out['status'] == 10002:
            return 1
        elif dict_out['data'] is None or dict_out['data'] == '':
            return 1
        elif dict_out['status'] == 0 and dict_out['data'] != '':
            return 0


def official_method():
    crawler = BaiduIndexActorAgeGenderCrawler()
    getor = get_url.GetUrl()
    actor_name_list = crawler.read_db_name()
    cfo = config_operator.ConfigOperator()
    offset = int(cfo.get_baidu_celebrity('gender_offset'))
    interval = int(cfo.get_baidu_celebrity('gender_interval'))
    for num in range(offset, actor_name_list.__len__()):
        name = actor_name_list[num][1]
        celebrity_id = actor_name_list[num][0]
        url = crawler.base_url + name
        try:
            response = getor.get_response(url)
            # print(response.text)
            if crawler.check_response_available(response.text):
                print('%s无数据' % celebrity_id)
                continue
            dict_result = crawler.get_info(response.text)
            crawler.write_db(dict_result, celebrity_id)
            cfo.write_baidu_celebrity('gender_offset', str(num))
        except Exception as e:
            while 1:
                try:
                    print('出现错误，30s后重试\n' + str(e))
                    time.sleep(30)
                    getor.change_account()
                    response = getor.get_response(url)
                    if crawler.check_response_available(response.text):
                        print('%s无数据' % celebrity_id)
                        break
                    dict_result = crawler.get_info(response.text)
                    crawler.write_db(dict_result, celebrity_id)
                    cfo.write_baidu_celebrity('gender_offset', str(num))
                    break
                except:
                    pass

        time.sleep(interval)


if __name__ == '__main__':
    official_method()
