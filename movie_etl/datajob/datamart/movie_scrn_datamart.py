from audioop import avg
from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import *


class MovieScrn:

    @classmethod
    def save(cls):
        movie_box_office = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        movie_detail = find_data(DataWarehouse, 'MOVIE_DETAIL')
        movie_hit = find_data(DataMart, 'MOVIE_HIT')

        movie_detail_open_scrn_df = movie_detail.join(
            movie_box_office, on=['MOVIE_CODE', 'MOVIE_NAME'], how='left')
        # movie_detail_open_show = movie_detail_open_show.select('MOVIE_CODE','MOVIE_NAME','SHOW_CNT')\
        #     .where(movie_detail_open_show.OPEN_DATE == movie_detail_open_show.STD_DATE)
        select_open_scrn_df = movie_detail_open_scrn_df.select('MOVIE_CODE', 'MOVIE_NAME', movie_detail_open_scrn_df.SCRN_CNT.alias('OPEN_SCRN_CNT'))\
            .where(movie_detail_open_scrn_df.STD_DATE == '20220801')

        avg_scrn_df = movie_box_office.groupby(
            movie_box_office.MOVIE_CODE).agg(avg('SCRN_CNT').alias('AVG_SCRN_CNT'))
        open_scrn_avg_df = select_open_scrn_df.join(
            avg_scrn_df, on='MOVIE_CODE', how='left')

        hit_select_df = movie_hit.select('MOVIE_CODE', 'HIT_GRADE')

        movie_scrn_df = open_scrn_avg_df.join(
            hit_select_df, on='MOVIE_CODE', how='left')

        movie_scrn_df.show()

        save_data(DataMart, movie_scrn_df, 'MOVIE_SCRN')
