from multiprocessing import get_logger
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import pandas as pd

from infra.hdfs_client import get_client
from infra.util import cal_std_day_yyyymmdd, execute_rest_api


class DailyBoxOfficeExtractor:
    URL = 'http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json'
    SERVICE_KEY = '3ad2d800ada4ee67f4d049b66e903680'
    FILE_DIR = '/movie/daily_box_office/'

    @classmethod
    def extract_data(cls, befor_cnt=1):

        for i in range(1, befor_cnt+1):
            # target_date = '20180101'
            params = cls.__create_param(i)

            try:
                res = execute_rest_api('get', cls.URL, {}, params)
                file_name = 'daliy_box_office_' + params['targetDt'] + '.json'
                cls.__upload_to_hdfs(file_name, res)

            except Exception as e:
                log_dict = cls.__create_log_dict(params)
                cls.__dump_log(log_dict, e)
                raise e

    @classmethod
    def __create_param(cls, befor_day):
        return {
            'key': cls.SERVICE_KEY,
            'targetDt': cal_std_day_yyyymmdd(befor_day)
        }

    @classmethod
    def __upload_to_hdfs(cls, file_name, res):
        get_client().write(cls.FILE_DIR + file_name, res, encoding='utf-8', overwrite=True)

    @classmethod
    def __dump_log(cls, log_dict, e):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        get_logger('boxoffice_extractor').error(log_json)

    @classmethod
    def __create_log_dict(cls, params):
        log_dict = {
            'is_success': 'Fail',
            'type': 'movie_detail',
            'params': params
        }

        return log_dict

    @classmethod
    def __date_string(cls, date):
        date_list = str(date).split()[0].split('-')
        date_str = date_list[0]+date_list[1]+date_list[2]
        return date_str


''' 상엽님 코드
        date = datetime(2018, 2, 1)
        date_str = cls.__date_string(date)

        print(date_str)

        while date_str != '20180101':
            date = date - timedelta(days=1)
            date_str = cls.__date_string(date)

            url = 'http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json?key=3ad2d800ada4ee67f4d049b66e903680&targetDt=' + date_str
            res = requests.get(url)
            print(res)
    
            text = res.text

            d = json.loads(text)
            print(d)
            movie_list = []

            x = d['boxOfficeResult']['showRange']
            print(x)
            for b in d['boxOfficeResult']['dailyBoxOfficeList']:
                movie_list.append([x, b['rnum'], b['rank'], b['rankInten'], b['rankOldAndNew'], b['movieCd'], b['movieNm'], b['openDt'], b['salesAmt'],
                                   b['salesShare'], b['salesInten'], b['salesChange'], b['salesAcc'], b['audiCnt'], b['audiInten'], b['audiChange'], b['audiAcc'], b['scrnCnt'], b['showCnt']])
            data = pd.DataFrame(movie_list)
            data.to_csv("movie_list.csv", mode='a',
                        encoding='cp949', index=False)
'''
