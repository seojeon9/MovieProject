from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import *
from pyspark.sql import *
from datetime import datetime, timedelta


class MovieRank:

    @classmethod
    def save(cls):
        movie_detail = find_data(DataWarehouse, 'MOVIE_DETAIL')
        dbo_table = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        movie_hit = find_data(DataMart, 'MOVIE_HIT')

        detail = movie_detail.select('MOVIE_CODE', 'MOVIE_NAME', 'OPEN_DATE')
        # detail.show()
        daily_box = dbo_table.select('MOVIE_CODE', 'MOVIE_NAME', 'STD_DATE', 'RANK')
        # daily_box.show()
        hit = movie_hit.select('MOVIE_CODE', 'HIT_GRADE')
        # hit.show()

        # 박스오피스 무비디테일 join
        box_detail = detail.join(daily_box, on=['MOVIE_CODE', 'MOVIE_NAME'], how='left')
        # box_detail.show()

        # 개봉일의 랭킹
        std_open = box_detail.select('MOVIE_CODE', 'MOVIE_NAME', box_detail.RANK.alias('FIRST_RANK')) \
                        .where(box_detail.STD_DATE == box_detail.OPEN_DATE) # 후에 box_detail.OPEN_DATE 로 변경
        # std_open.show()

        # 개봉 2주차의 랭킹(개봉일 +7일)
        std_second = box_detail.select('MOVIE_CODE', 'MOVIE_NAME', box_detail.RANK.alias('SECOND_RANK')) \
                        .where(box_detail.STD_DATE == box_detail.OPEN_DATE+7) # 후에 box_detail.OPEN_DATE+7 로 변경
        # std_second.show()

        # 개봉일과 개봉 2주차 랭킹 join
        open_second_join = std_open.join(std_second, on=['MOVIE_CODE', 'MOVIE_NAME'])
        # open_second_join.show()

        # 개봉일 랭킹 1위 여부
        std_open_yn = std_open.withColumn('OPEN_FIRST_RANK_YN'
                                            , when(std_open.FIRST_RANK == '1', 'Y').otherwise('N'))
        # std_open_yn.show()

        # 개봉 2주차 순위 하락 여부
        drop_second_yn = open_second_join.withColumn('SEC_WEEK_RANK_DROP'
                                                , when(open_second_join.FIRST_RANK < open_second_join.SECOND_RANK, 'Y').otherwise('N'))
        # drop_second_yn.show()

        # 개봉일 랭킹 1위 여부 / 개봉 2주차 순위 하락 여부 join
        merge_yn_df = std_open_yn.join(drop_second_yn, on=['MOVIE_CODE', 'MOVIE_NAME', 'FIRST_RANK'])
        # merge_yn_df.show()

        # 흥행등급 추가
        merge_hit = merge_yn_df.join(hit, on='MOVIE_CODE')
        # merge_hit.show()

        # 최종 반환 열만 남기고 제거
        movie_rank = merge_hit.drop('FIRST_RANK', 'SECOND_RANK')
        movie_rank.show()

        # DataMart에 저장
        save_data(DataMart, movie_rank, 'MOVIE_RANK')
