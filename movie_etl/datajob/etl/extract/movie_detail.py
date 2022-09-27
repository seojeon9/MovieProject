from infra.hdfs_client import get_client
from infra.util import execute_rest_api
import json
from multiprocessing import get_logger


class MovieDetailApiExtractor:
    URL = 'http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json'
    SERVICE_KEY = '6cdee3cbd6d29e49fdc6bd17a2feb85b'
    FILE_DIR = '/movie_data/detail/'

    @classmethod
    def extract_data(cls):
        params = cls.__create_param()

        try:
            res = execute_rest_api('get', cls.URL, {}, params)
            file_name = 'movie_detail_' + params['movieCd'] + '.json'
            cls.__upload_to_hdfs(file_name, res)

        except Exception as e:
            log_dict = cls.__create_log_dict(params)
            cls.__dump_log(log_dict, e)
            raise e

    @classmethod
    def __create_param(cls):
        return {
            'key': cls.SERVICE_KEY,
            'movieCd': '20215601'
        }

    @classmethod
    def __upload_to_hdfs(cls, file_name, res):
        get_client().write(cls.FILE_DIR + file_name, res, encoding='utf-8', overwrite=True)

    @classmethod
    def __dump_log(cls, log_dict, e):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        get_logger('movie_extractor').error(log_json)

    @classmethod
    def __create_log_dict(cls, params):
        log_dict = {
            'is_success': 'Fail',
            'type': 'movie_detail',
            'params': params
        }

        return log_dict
