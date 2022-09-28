import json
from multiprocessing import get_logger
from infra.hdfs_client import get_client
import urllib.request

from infra.jdbc import DataWarehouse, find_data


class NaverSearchMovieExtractor:

    URL = 'https://openapi.naver.com/v1/search/movie.json?query='
    CLIENT_ID = 'pOdfwRraC5W55dxPeRPQ'
    CLIENT_KEY = '8jQTYZWryv'
    FILE_DIR = '/naver/search_movie/'

    movie_code = []
    movie_name = []

    @classmethod
    def extract_data(cls):

        keyword, params = cls.__extract_keyword()
        encText = urllib.parse.quote(keyword)
        url = cls.URL + encText
        request = urllib.request.Request(url)
        request.add_header("X-Naver-Client-Id", cls.CLIENT_ID)
        request.add_header("X-Naver-Client-Secret", cls.CLIENT_KEY)
        response = urllib.request.urlopen(request)
        rescode = response.getcode()

        try:
            if (rescode == 200):
                response_body = response.read()
                res = response_body.decode('utf-8')
                print(res)
                keyword = keyword.replace(':', '_')
                file_name = 'naver_search_movie_' + keyword + '.json'
                cls.__upload_to_hdfs(file_name, res)
            else:
                print("Error Code:" + rescode)
        except Exception as e:
            print('예외발생 : ', e)
            log_dict = cls.__create_log_dict(params)
            cls.__dump_log(log_dict, e)
            raise e

    @classmethod
    def f(cls, movie):
        cls.movie_code.append(movie.MOVIE_CODE)
        cls.movie_name.append(movie.MOVIE_NAME)
        print(movie.MOVIE_CODE)
        print(movie.MOVIE_NAME)

    @classmethod
    def __extract_keyword(cls):
        # 저장된 DW 박스오피스 영화 테이블에서 영화명, 코드 빼와서 영화명으로 검색
        keyword = '공조2: 인터내셔날'
        params = {
            'query': keyword
        }
        return keyword, params

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
