from audioop import avg
from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import *


class MovieShow:

    @classmethod
    def save(cls):
        movie_box_office = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        movie_detail = find_data(DataWarehouse, 'MOVIE_DETAIL')
        movie_hit = find_data(DataMart, 'MOVIE_HIT')

        movie_detail_open_show_df = movie_detail.join(
            movie_box_office, on=['MOVIE_CODE', 'MOVIE_NAME'], how='left')
        # movie_detail_open_show = movie_detail_open_show.select('MOVIE_CODE','MOVIE_NAME','SHOW_CNT')\
        #     .where(movie_detail_open_show.OPEN_DATE == movie_detail_open_show.STD_DATE)
        select_open_show_df = movie_detail_open_show_df.select('MOVIE_CODE', 'MOVIE_NAME', movie_detail_open_show_df.SHOW_CNT.alias('OPEN_SHOW_CNT'))\
            .where(movie_detail_open_show_df.STD_DATE == '20220801')

        avg_show_df = movie_box_office.groupby(
            movie_box_office.MOVIE_CODE).agg(avg('SHOW_CNT').alias('AVG_SHOW_CNT'))
        open_show_avg_df = select_open_show_df.join(
            avg_show_df, on='MOVIE_CODE', how='left')

        hit_select_df = movie_hit.select('MOVIE_CODE', 'HIT_GRADE')

        movie_show_df = open_show_avg_df.join(
            hit_select_df, on='MOVIE_CODE', how='left')

        movie_show_df.show()

        save_data(DataMart, movie_show_df, 'MOVIE_SHOW')
