from audioop import avg
from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import *


class MovieScrn:

    @classmethod
    def save(cls):
        movie_box_office = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        movie_detail = find_data(DataWarehouse, 'MOVIE_DETAIL')
        movie_hit = find_data(DataMart, 'MOVIE_HIT')

        select_open_scrn_df = cls.__select_open_scrn_df(
            movie_box_office, movie_detail)

        open_scrn_avg_df = cls.__groupby_avg_scrn_df(
            movie_box_office, select_open_scrn_df)

        movie_scrn_df = cls.__join_hit_grade(movie_hit, open_scrn_avg_df)

        save_data(DataMart, movie_scrn_df, 'MOVIE_SCRN')

    @classmethod
    def __join_hit_grade(cls, movie_hit, open_scrn_avg_df):
        hit_select_df = movie_hit.select('MOVIE_CODE', 'HIT_GRADE')

        movie_scrn_df = open_scrn_avg_df.join(
            hit_select_df, on='MOVIE_CODE', how='left')

        return movie_scrn_df

    @classmethod
    def __groupby_avg_scrn_df(cls, movie_box_office, select_open_scrn_df):
        avg_scrn_df = movie_box_office.groupby(
            movie_box_office.MOVIE_CODE).agg(avg('SCRN_CNT').alias('AVG_SCRN_CNT'))
        open_scrn_avg_df = select_open_scrn_df.join(
            avg_scrn_df, on='MOVIE_CODE', how='left')

        return open_scrn_avg_df

    @classmethod
    def __select_open_scrn_df(cls, movie_box_office, movie_detail):
        movie_detail_open_scrn_df = movie_detail.join(
            movie_box_office, on=['MOVIE_CODE', 'MOVIE_NAME'], how='left')
        select_open_scrn_df = movie_detail_open_scrn_df.select('MOVIE_CODE', 'MOVIE_NAME', movie_detail_open_scrn_df.SCRN_CNT.alias('OPEN_SCRN_CNT'))\
            .where(movie_detail_open_scrn_df.OPEN_DATE == movie_detail_open_scrn_df.STD_DATE)

        return select_open_scrn_df
