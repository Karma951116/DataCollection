# 获取猫眼影片评分和想看数
import requests
import json
import re
import time

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from Utils import mysql_operator
from Utils import config_operator


class FilmRatingCrawler:
    def __init__(self):
        self.rating_url = 'http://piaofang.maoyan.com/movie/-/audienceRating'
        self.wanted_url = 'http://piaofang.maoyan.com/movie/-/wantindex'
        self.operator = mysql_operator.MysqlOperator()

    def get_rating_response(self, film_id):
        # 伪装请求
        ua = UserAgent()
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
            'Connection': 'keep-alive',
            'Referer': 'https://piaofang.maoyan.com/movie/%s/promotion/trailers' % film_id,
            'Cookie': '__mta=217831081.1608186388084.1611832389971.1611832764462.110; _lxsdk_cuid=1766f5f7d67c8-0624e350695b8c-c791039-1fa400-1766f5f7d67c8; recentCis=1%3D151%3D140%3D84%3D197; theme=moviepro; _bsin_180503_=[1,10,11,12,13,14,15,16,17,2,3,4,5,6,7,8,9]; _lxsdk=7B68F3905C8A11EBB5F151AF051690DE1D7402F0C99D456A8D6E3BB78EA2E993; Hm_lvt_703e94591e87be68cc8da0da7cbd0be2=1611282588,1611303543,1611303548,1611303555; __mta=217831081.1608186388084.1611817639496.1611819045156.107; __mta=217831081.1608186388084.1611819045156.1611828685079.108; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; Hm_lpvt_703e94591e87be68cc8da0da7cbd0be2=1611896259; _lxsdk_s=1774e69f736-905-b9a-b49%7C1097461883%7C1',
            'User-Agent': ua.random
        }
        cur_url = self.rating_url.replace('-',  film_id.__str__())
        response = requests.get(url=cur_url, headers=headers)
        if response.status_code is not 200:
            return 1
        return response.text

    def get_wanted_response(self, film_id):
        # 伪装请求
        ua = UserAgent()
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;'
                      'q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
            'Connection': 'keep-alive',
            'Cookie': '__mta=217831081.1608186388084.1611928647805.1612143427051.113; _lxsdk_cuid=1766f5f7d67c8-0624e350695b8c-c791039-1fa400-1766f5f7d67c8; recentCis=1%3D151%3D140%3D84%3D197; theme=moviepro; _bsin_180503_=[1,10,11,12,13,14,15,16,17,2,3,4,5,6,7,8,9]; _lxsdk=7B68F3905C8A11EBB5F151AF051690DE1D7402F0C99D456A8D6E3BB78EA2E993; Hm_lvt_703e94591e87be68cc8da0da7cbd0be2=1611282588,1611303543,1611303548,1611303555; __mta=217831081.1608186388084.1611817639496.1611819045156.107; __mta=217831081.1608186388084.1611819045156.1611828685079.108; Hm_lpvt_703e94591e87be68cc8da0da7cbd0be2=1612057932; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; _lxsdk_s=1775b923ad9-cd4-a27-445%7C%7C1',
            'User-Agent': ua.random,
            'Referer': 'https://piaofang.maoyan.com/movie/%s/promotion/trailers' % film_id
        }
        cur_url = self.wanted_url.replace('-',  film_id.__str__())
        response = requests.get(url=cur_url, headers=headers)
        if response.status_code is not 200:
            return 1
        return response.text

    def read_film_id(self):
        self.operator.conn_mysql()
        sql = 'select filmId from maoyan_film_baseinfo where (year = 2020 or year = 2019) and showCountry = "中国大陆"'
        result = self.operator.search(sql)
        if result == 1:
            print('从数据库读取影片Id出错')
        return result

    def get_ratings(self, html_doc):
        bs = BeautifulSoup(html_doc, 'html.parser')
        span_score = bs.find(class_='score-num')
        # 评分
        maoyan_score = ''
        if span_score is not None:
            maoyan_score = span_score.text
            if maoyan_score is not None and maoyan_score != '':
                maoyan_score = float(maoyan_score)
        # 星级
        linebar_list = bs.find_all(class_='linebars-item')
        score_dict = {}
        if linebar_list is not None:
            for item in linebar_list:
                title_object = item.find(class_='linebars-num')
                if title_object is None:
                    continue
                title = title_object.text
                value_object = item.find(class_='linebars-value')
                if value_object is None:
                    continue
                value = value_object.text.replace('%', '')
                if value == '- -':
                    continue
                value = (float(value) / 100)
                value = '%.3f' % value
                score_dict[title] = value

        type_list = ['9-10分', '7-8分', '5-6分', '3-4分', '1-2分']
        # 检查各项评分是否都存在，防止写入数据库出错
        if score_dict.__len__() < 5:
            for item in type_list:
                flag = False
                for key in score_dict:
                    if str(key) == item:
                        flag = True
                if flag is False:
                    score_dict[item] = ''

        # 评分人数
        comment_num_block = bs.find(class_='score-people-count')
        comment_num = ''
        if comment_num_block is not None:
            comment_num_str = comment_num_block.text.replace('< !-- -->', '')
            comment_num_str = comment_num_block.text.replace('人评分', '')
            search_result = re.search(r'\d+.\d+', comment_num_str)
            unit = ''
            if search_result is not None:
                comment_num = search_result.group(0)

            search_result = re.search(r'[\u4E00-\u9FA5\s]+', comment_num_str)
            if search_result is not None:
                unit = search_result.group(0)
            if comment_num is not None and unit is not None:
                if unit == '':
                    comment_num = comment_num
                else:
                    comment_num = self.unit_convert(float(comment_num), unit)

        # betterthan
        betterthan_list = bs.find_all(class_='better-than-item')
        betterthan = ''
        if betterthan_list is not None:
            for item in betterthan_list:
                betterthan += item.text + ';'

        dict_rating_info = {}
        dict_rating_info['comment_num'] = comment_num
        dict_rating_info['comparison'] = betterthan
        dict_rating_info['maoyan_score'] = maoyan_score
        dict_rating_info['score_dict'] = score_dict

        if comment_num == '' and betterthan == '' and maoyan_score == '':
            return None

        return dict_rating_info

    def get_wanted(self, dict_data_wanted):
        if self.check_data_wanted(dict_data_wanted):
            return None

        page_data = dict_data_wanted['pageData']
        if page_data is None:
            return

        add_want = page_data['addWant']
        wish_info = add_want['wishInfo']
        count = wish_info['count']
        unit = None
        try:
            unit = wish_info['unit']
        except:
            ret_num = float(count)

        if unit is None or unit == '':
            ret_num = float(count)
        else:
            ret_num = self.unit_convert(float(count), unit)
        return float(ret_num)

    def get_datadict_fromscript(self, html_doc):
        bs = BeautifulSoup(html_doc, 'html.parser')
        # 筛选出包含数据的script
        script_list = bs.find_all('script')
        script_str = None
        for script in script_list:
            temp = script.string
            if temp is not None and 'AppData' in script.string:
                script_str = script.string.replace('var AppData = ', '').replace(';', '', -1)
                break

        if script_str is None:
            return
        dict_data = json.loads(script_str)
        return dict_data

    def check_data_rating(self, dict_data):
        if dict_data['pageData']['isShow'] is None or dict_data['pageData']['isShow'] is False:
            return 1
        else:
            return 0

    def check_data_wanted(self, dict_data_wanted):
        if dict_data_wanted['pageData']['addWant'] is None or dict_data_wanted['pageData']['addWant'] == {}:
            return 1
        else:
            return 0

    def unit_convert(self, num, unit):
        # 单位转换到万
        ret_num = 0
        if unit == '亿':
            ret_num = num * 10000 * 10000
        elif unit == '万':
            ret_num = num * 10000
        elif unit == '千':
            ret_num = num * 1000
        return ret_num

    def write_db(self, dict_rating_info, want_num, film_id):
        if dict_rating_info is None and want_num is None:
            print('%d无数据' % film_id)
            return
        rating_info = ''
        sql = 'select * from maoyan_film_ratings where filmId=%d' % film_id
        if self.operator.search(sql).__len__() > 0:
            sql = ''
            if dict_rating_info is None:
                sql = 'update maoyan_film_ratings set wanted="%s" where filmId=%d' % (want_num, film_id)
            elif want_num is None:
                sql = 'update maoyan_film_ratings set rating="%s", ratingNum="%s", fiveStars="%s", fourStars="%s", threeStars="%s",' \
                      'twoStars="%s", oneStar="%s", betterthan="%s" where filmId=%d' % \
                      (dict_rating_info['maoyan_score'], dict_rating_info['comment_num'],
                       dict_rating_info['score_dict']['9-10分'],
                       dict_rating_info['score_dict']['7-8分'], dict_rating_info['score_dict']['5-6分'],
                       dict_rating_info['score_dict']['3-4分'],
                       dict_rating_info['score_dict']['1-2分'], dict_rating_info['comparison'], film_id)
            else:
                sql = 'update maoyan_film_ratings set rating="%s", ratingNum="%s", fiveStars="%s", fourStars="%s", threeStars="%s",' \
                      'twoStars="%s", oneStar="%s", betterthan="%s", wanted="%s" where filmId=%d' % \
                      (dict_rating_info['maoyan_score'], dict_rating_info['comment_num'],
                       dict_rating_info['score_dict']['9-10分'],
                       dict_rating_info['score_dict']['7-8分'], dict_rating_info['score_dict']['5-6分'],
                       dict_rating_info['score_dict']['3-4分'],
                       dict_rating_info['score_dict']['1-2分'], dict_rating_info['comparison'], want_num, film_id)

            if self.operator.execute_sql(sql) == 0:
                rating_info = '已更新'
            else:
                rating_info = '更新失败'
        else:
            if dict_rating_info is None:
                sql = 'insert into maoyan_film_ratings (filmId, wanted) values(%d, "%s")' % (film_id, want_num)
            elif want_num is None:
                sql = 'insert into maoyan_film_ratings (filmId, rating, ratingNum, fiveStars, fourStars, threeStars,' \
                      'twoStars, oneStar, betterthan) ' \
                      'values(%d, "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")' % \
                      (film_id, dict_rating_info['maoyan_score'], dict_rating_info['comment_num'],
                       dict_rating_info['score_dict']['9-10分'],
                       dict_rating_info['score_dict']['7-8分'], dict_rating_info['score_dict']['5-6分'],
                       dict_rating_info['score_dict']['3-4分'],
                       dict_rating_info['score_dict']['1-2分'], dict_rating_info['comparison'])
            else:
                sql = 'insert into maoyan_film_ratings (filmId, rating, ratingNum, fiveStars, fourStars, threeStars,' \
                      'twoStars, oneStar, betterthan, wanted) ' \
                      'values(%d, "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")' % \
                      (film_id, dict_rating_info['maoyan_score'], dict_rating_info['comment_num'],
                       dict_rating_info['score_dict']['9-10分'],
                       dict_rating_info['score_dict']['7-8分'], dict_rating_info['score_dict']['5-6分'],
                       dict_rating_info['score_dict']['3-4分'],
                       dict_rating_info['score_dict']['1-2分'], dict_rating_info['comparison'], want_num)

            if self.operator.execute_sql(sql) == 0:
                rating_info = '成功'
            else:
                rating_info = '失败'

        print('%s;rating_info%s' % (film_id, rating_info))


def official_method():
    crawler = FilmRatingCrawler()
    film_id_list = crawler.read_film_id()
    cfo = config_operator.ConfigOperator()
    offset = int(cfo.get_maoyan_film('rating_offset'))
    interval = int(cfo.get_maoyan_film('rating_interval'))
    for num in range(offset, film_id_list.__len__()):
        film_id = int(film_id_list[num][0])
        # 获取评分信息
        html_doc = crawler.get_rating_response(film_id)
        dict_data = crawler.get_datadict_fromscript(html_doc)
        dict_rating_info = crawler.get_ratings(html_doc)
        # 获取想看数
        html_doc_wanted = crawler.get_wanted_response(film_id)
        #print(html_doc_wanted)
        dict_data_wanted = crawler.get_datadict_fromscript(html_doc_wanted)
        want_num = crawler.get_wanted(dict_data_wanted)
        crawler.write_db(dict_rating_info, want_num, film_id)
        cfo.write_maoyan_film('rating_offset', num.__str__())
        time.sleep(interval)


if __name__ == '__main__':
    official_method()


'''     # 评分备选方案
        if self.check_data_rating(dict_data):
            return None

        page_data = dict_data['pageData']
        if page_data is None:
            return
        basic_info = page_data['basicInfo']
        score_list = page_data['scoreDist']
        comment_num = basic_info['commentNum']
        comparison = basic_info['comparison']
        maoyan_score = basic_info['maoyanScore']
        # 数值处理
        search_result = re.search(r'\d+.\d+', comment_num)
        num = None
        unit = None
        if search_result is not None:
            num = search_result.group(0)

        search_result = re.search(r'[\u4E00-\u9FA5\s]+', comment_num)
        if search_result is not None:
            unit = search_result.group(0)

        if num is not None and unit is not None:
            if unit == '':
                comment_num = num
            else:
                comment_num = self.unit_convert(float(num), unit)


        temp = ''
        for item in comparison:
            temp += str(item) + ';'
        temp = temp.strip()
        comparison = temp

        score_dict = {}
        type_list = ['9-10分', '7-8分', '5-6分', '3-4分', '1-2分']
        for item in score_list:
            score_dict[item['type']] = item['value']
        # 检查各项评分是否都存在，防止写入数据库出错
        if score_dict.__len__() < 5:
            for item in type_list:
                flag = False
                for key in score_dict:
                    if str(key) == item:
                        flag = True
                if flag is False:
                    score_dict[item] = ''

        dict_rating_info = {}
        dict_rating_info['comment_num'] = float(comment_num)
        dict_rating_info['comparison'] = comparison
        dict_rating_info['maoyan_score'] = float(maoyan_score)
        dict_rating_info['score_dict'] = score_dict

        return dict_rating_info
        '''