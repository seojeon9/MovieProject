from datetime import datetime, timedelta
import json
from multiprocessing import get_logger
from infra.hdfs_client import get_client
import urllib.request

from infra.jdbc import DataWarehouse, find_data


class NaverDatalabApiExtractor:

    URL = 'https://openapi.naver.com/v1/datalab/search'
    CLIENT_ID = 'pOdfwRraC5W55dxPeRPQ'
    CLIENT_KEY = '8jQTYZWryv'
    FILE_DIR = '/naver/datalab/'

    movie_codes = []
    movie_names = []
    open_dates = []

    @classmethod
    def extract_data(cls):

        data = find_data(DataWarehouse, 'MOVIE_DETAIL')
        # print(type(data))  # pyspark.sql.dataframe.DataFrame
        data = data.select('MOVIE_CODE', 'MOVIE_NAME', 'OPEN_DATE')
        data = data.to_pandas_on_spark()
        for dt in data.values:
            print(dt)
            cls.movie_codes.append(dt[0])
            cls.movie_names.append(dt[1])
            cls.open_dates.append(dt[2])

        for i in range(len(cls.movie_codes)):
            code = cls.movie_codes[i]
            start_date = cls.open_dates[i]
            print(start_date)
            date = datetime.strptime(start_date, '%Y%m%d')
            start_date = date.strftime("%Y-%m-%d")
            end_date = date + timedelta(weeks=15)
            if end_date > datetime.now():
                end_date = datetime.now()
            end_date = end_date.strftime("%Y-%m-%d")
            time_unit = 'week'  # date, week, month
            name = cls.movie_names[i]
            keword = name.split(':')[0]
            # body = "{\"startDate\":\"2022-09-01\",\"endDate\":\"2022-09-28\",\"timeUnit\":\"week\",\"keywordGroups\":[{\"groupName\":\"공조2: 인터내셔날\",\"keywords\":[\"공조2: 인터내셔날\",\"공조2\"]}]}"
            body = '{\"startDate\":\"' + start_date + '\",\"endDate\":\"' + end_date + '\",\"timeUnit\":\"' + \
                time_unit + \
                '\",\"keywordGroups\":[{\"groupName\":\"' + \
                name + '\",\"keywords\":[\"' + \
                name + '\",\"' + keword + '\"]}]}'

            print(body)
            params = {
                'body': body
            }

            request = urllib.request.Request(cls.URL)
            request.add_header("X-Naver-Client-Id", cls.CLIENT_ID)
            request.add_header("X-Naver-Client-Secret", cls.CLIENT_KEY)
            request.add_header("Content-Type", "application/json")
            response = urllib.request.urlopen(
                request, data=body.encode("utf-8"))
            rescode = response.getcode()

            try:
                if (rescode == 200):
                    response_body = response.read()
                    res = response_body.decode('utf-8')
                    print(res)
                    file_name = 'naver_datalab_' + code + '.json'
                    cls.__upload_to_hdfs(file_name, res)
                else:
                    print("Error Code:" + rescode)
            except Exception as e:
                print('예외발생 : ', e)
                log_dict = cls.__create_log_dict(params)
                cls.__dump_log(log_dict, e)
                raise e

    @classmethod
    def __upload_to_hdfs(cls, file_name, res):
        get_client().write(cls.FILE_DIR + file_name, res, encoding='utf-8', overwrite=True)

    @classmethod
    def __dump_log(cls, log_dict, e):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        get_logger().error(log_json)
        # get_logger('naver_search_movie_extractor').error(log_json)

    @classmethod
    def __create_log_dict(cls, params):
        log_dict = {
            'is_success': 'Fail',
            'type': 'naver_search_movie',
            'params': params
        }

        return log_dict
