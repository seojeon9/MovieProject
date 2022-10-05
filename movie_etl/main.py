import sys
from datajob.etl.extract.daily_boxoffice_api import DailyBoxofficeExtractor
from datajob.etl.extract.movie_detail import MovieDetailApiExtractor
from datajob.etl.extract.movie_score import MovieScoreExtractor
from datajob.etl.extract.naver_datalab_api import NaverDatalabApiExtractor
from datajob.etl.extract.naver_search_api import NaverSearchMovieExtractor
from datajob.etl.transform.daily_boxoffice_transform import DailyBoxOfficeTransformer
from datajob.etl.transform.movie_detail_transform import MovieDetailTransformer
from datajob.etl.transform.movie_score_transform import MovieScoreTransformer
from datajob.etl.transform.movie_search_ratio_transform import MovieSearchRatioTransfomer
from datajob.etl.transform.movie_url_and_actors_transform import MovieUrlAndActorsTransformer


def transform_execute():
    CoronaPatientTransformer.transform()
    CoronaVaccineTransformer.transform()

def datamart_execute():
    CoPopuDensity.save()
    CoVaccine.save()

works = {
    'extract':{
        'daily_boxoffice': DailyBoxofficeExtractor.extract_data,
        'movie_detail':MovieDetailApiExtractor.extract_data,
        'movie_score':MovieScoreExtractor.extract_data,
        'naver_datalab':NaverDatalabApiExtractor.extract_data,
        'naver_search':NaverSearchMovieExtractor.extract_data,
    },
    'transform':{
        'daily_boxoffice':DailyBoxOfficeTransformer.transform,
        'movie_detail':MovieDetailTransformer.transform,
        'movie_score':MovieScoreTransformer.transform,
        'naver_search_ratio':MovieSearchRatioTransfomer.transform,
        'movie_url_actors':MovieUrlAndActorsTransformer.transform,
        'execute':transform_execute,
    },
    'datamart':{
        'execute':datamart_execute,
        'co_popu_density':CoPopuDensity.save,
        'co_vaccine':CoVaccine.save
    }

}

if __name__ == "__main__":
    args = sys.argv
    print(args)

    # main.py 작업(extract, transform, datamart) 저장할 위치(테이블)
    # 매개변수 2개
    if len(args) != 3:
        raise Exception('2개의 전달인자가 필요합니다.')

    if args[1] not in works.keys():
        raise Exception('첫번째 전달인자가 이상함 >> ' + str(works.keys()))

    if args[2] not in works[args[1]].keys():
        raise Exception('두번째 전달인자가 이상함 >> ' + str(works[args[1]].keys()))

    work = works[args[1]][args[2]]
    work()
