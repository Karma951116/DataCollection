import requests
import time
import json

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from Utils import mysql_operator
from Utils import config_operator
from Utils import get_url


class FilmBoxOfficeCrawler:
    def __init__(self):
        self.base_url = 'http://piaofang.maoyan.com/movie/-/boxshow'
        self.operator = mysql_operator.MysqlOperator()

    def read_film_id(self):
        self.operator.conn_mysql()
        sql = 'select filmId from maoyan_film_baseinfo where (year = 2020 or year = 2019) and (FIND_IN_SET("中国大陆",' \
              ' productCountry) or FIND_IN_SET("中国台湾", productCountry) or FIND_IN_SET("中国香港", productCountry))'
        result = self.operator.search(sql)
        if result == 1:
            print('从数据库读取影片Id出错')
        return result

    def get_summary_boxoffice(self, dict_data):
        # 具体获取内容查看script(包含一个 var AppData json字符串的script)经过json格式化后的数据
        box_info_data = dict_data['boxInfoData']
        if box_info_data is None:
            return
        summary_list = box_info_data[0]['boxSummaryList']
        if summary_list is None:
            return
        dict_summary_info = {}
        dict_summary_info['累计综合票房'] = ''
        dict_summary_info['累计分账票房'] = ''
        dict_summary_info['首日综合票房'] = ''
        dict_summary_info['首周综合票房'] = ''
        for item in summary_list:
            title = item['title']
            valueDesc = item['valueDesc']
            unitDesc = ''
            if valueDesc is not None and valueDesc != '--':
                valueDesc = float(item['valueDesc'])
                unitDesc = item['unitDesc']

            dict_summary_info[title] = round(self.unit_convert(valueDesc, unitDesc), 2)

        return dict_summary_info

    def get_day_boxoffice(self, dict_data):
        data_list = dict_data['boxShowData'][0]['data']
        if data_list is None:
            return

        dict_day_boxoffice = {}
        for item in data_list:
            box_office = item['boxDesc']
            split_box_office = item['splitBoxDesc']
            # avgv_view_box = item['avgViewBoxDesc']               # 综合票价
            # avgv_view_splitbox = item['avgViewSplitBoxDesc']     # 分账票价
            # avg_show_view = item['avgShowViewDesc']              # 场均观影人次
            # show_count = item['showCount']                       # 场次
            show_date = str(item['showDate'])

            dict_data_after = {}
            dict_data_after['box_office'] = float(box_office)
            dict_data_after['split_box_office'] = float(split_box_office)
            # dict_data_after['avgv_view_box'] = avgv_view_box
            # dict_data_after['avgv_view_splitbox'] = avgv_view_splitbox
            # dict_data_after['avg_show_view'] = avg_show_view
            # dict_data_after['show_count'] = show_count
            # 如果值为'--'则替换为空
            for key in dict_data_after:
                if '--' in str(dict_data_after[key]):
                    dict_data_after[key] = ''

            year = show_date[0:4]
            month = show_date[4:6]
            day = show_date[6:8]
            show_date = year + '-' + month + '-' + day
            dict_day_boxoffice[show_date] = dict_data_after
        return dict_day_boxoffice

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

    def unit_convert(self, num, unit):
        # 单位转换到万
        ret_num = 0
        if unit == '亿':
            ret_num = num * 10000
        elif unit == '万':
            ret_num = num
        elif unit == '千':
            ret_num = num / 10
        return ret_num

    def write_db(self, dict_summary_info, dict_day_boxoffice, film_id):
        # 影片综合票房信息查重
        summary_info = ''
        sql = 'select * from maoyan_film_boxsummary where filmId=%d' % film_id
        if self.operator.search(sql).__len__() > 0:
            sql = 'update maoyan_film_boxsummary set boxOffice="%s", splitBoxOffice="%s", boxOfficeFirstDay="%s", boxOfficeFirstWeek="%s" ' \
                  'where filmId=%d' % \
                  (dict_summary_info['累计综合票房'], dict_summary_info['累计分账票房'], dict_summary_info['首日综合票房'],
                   dict_summary_info['首周综合票房'], film_id)

            if self.operator.execute_sql(sql) == 0:
                summary_info = '已更新'
            else:
                summary_info = '更新失败'
        else:
            sql = 'insert into maoyan_film_boxsummary (filmId, boxOffice, splitBoxOffice, boxOfficeFirstDay, boxOfficeFirstWeek) ' \
                  'values(%d, "%s", "%s", "%s", "%s")' % \
                  (film_id, dict_summary_info['累计综合票房'], dict_summary_info['累计分账票房'], dict_summary_info['首日综合票房'],
                   dict_summary_info['首周综合票房'])

            if self.operator.execute_sql(sql) == 0:
                summary_info = '成功'
            else:
                summary_info = '失败'

        day_boxoffice_num = len(dict_day_boxoffice)
        day_boxoffice_already = 0
        day_boxoffice_succ = 0
        day_boxoffice_failed = 0
        for key in dict_day_boxoffice:
            sql = 'select * from maoyan_film_boxday where filmId="%d" and date="%s"' % (film_id, str(key))
            if self.operator.search(sql).__len__() > 0:
                sql = 'update maoyan_film_boxday set boxOffice=%f, splitBoxOffice=%f where filmId=%d and date="%s"' % \
                      (dict_day_boxoffice[key]['box_office'], dict_day_boxoffice[key]['split_box_office'], film_id, key)
                if self.operator.execute_sql(sql) == 0:
                    day_boxoffice_already += 1
                else:
                    day_boxoffice_failed += 1
            else:
                sql = 'insert into maoyan_film_boxday (filmId, date, boxOffice, splitBoxOffice)' \
                      'values(%d, "%s", %f, %f)' % (film_id, key, dict_day_boxoffice[key]['box_office'],
                                                  dict_day_boxoffice[key]['split_box_office'])
                if self.operator.execute_sql(sql) == 0:
                    day_boxoffice_succ += 1
                else:
                    day_boxoffice_failed += 1

        day_boxoffice = '[数量: %d,已更新: %d, 成功: %d, 失败: %d]' % \
                        (int(day_boxoffice_num), int(day_boxoffice_already), int(day_boxoffice_succ), int(day_boxoffice_failed))
        print('%s;summary_info%s;day_boxoffice%s' % (film_id, summary_info, day_boxoffice))

    def check_data(self, dict_data):
        if dict_data['boxInfoData'] is None or dict_data['boxInfoData'] == []:
            return 1
        else:
            return 0


def official_method():
    crawler = FilmBoxOfficeCrawler()
    getor = get_url.GetUrl()
    film_id_list = crawler.read_film_id()
    #print(film_id_list)
    cfo = config_operator.ConfigOperator()
    offset = int(cfo.get_maoyan_film('boxoffice_offset'))
    interval = int(cfo.get_maoyan_film('boxoffice_interval'))
    for num in range(offset, film_id_list.__len__()):
        try:
            film_id = int(film_id_list[num][0])
            url = crawler.base_url.replace('-', film_id.__str__())
            response = getor.get_response(url)
            # print(html_doc)
            dict_data = crawler.get_datadict_fromscript(response.text)
            if crawler.check_data(dict_data):
                print(str(film_id) + '无数据')
                cfo.write_maoyan_film('boxoffice_offset', num.__str__())
                time.sleep(interval)
                continue
            dict_summary_info = crawler.get_summary_boxoffice(dict_data)
            dict_day_boxoffice = crawler.get_day_boxoffice(dict_data)
            crawler.write_db(dict_summary_info, dict_day_boxoffice, film_id)
            cfo.write_maoyan_film('boxoffice_offset', num.__str__())
        except Exception as e:
            while 1:
                try:
                    print('出现错误，30s后重试\n' + str(e))
                    getor.change_account()
                    time.sleep(30)
                    response = getor.get_response(url)
                    # print(html_doc)
                    dict_data = crawler.get_datadict_fromscript(response.text)
                    if crawler.check_data(dict_data):
                        print(str(film_id) + '无数据')
                        cfo.write_maoyan_film('boxoffice_offset', num.__str__())
                        time.sleep(interval)
                        continue
                    dict_summary_info = crawler.get_summary_boxoffice(dict_data)
                    dict_day_boxoffice = crawler.get_day_boxoffice(dict_data)
                    crawler.write_db(dict_summary_info, dict_day_boxoffice, film_id)
                    cfo.write_maoyan_film('boxoffice_offset', num.__str__())
                except:
                    pass

        time.sleep(interval)


if __name__ == '__main__':
    official_method()

'''             # 从页面其他元素获取summary boxoffic
                bs = BeautifulSoup(html_doc, 'html.parser')
                summary_block = bs.find(class_='box-summary')
                if summary_block is None:
                    return
                div_list = summary_block.find_all(class_='box-item')
                if div_list is None:
                    return

                dict_summary_info = {}
                for div in div_list:
                    item_object = div.find(class_='box-desc')
                    item = ''
                    if item_object is not None:
                        item = item_object.text

                    num_object = div.find(class_='box-num')
                    num = 0
                    if num_object is not None:
                        num = float(num_object.text)

                    unit_object = div.find(class_='box-unit')
                    unit = ''
                    if unit_object is not None:
                        unit = unit_object.text

                    dict_summary_info[item] = round(self.unit_convert(num, unit), 2)

                return dict_summary_info
                '''
