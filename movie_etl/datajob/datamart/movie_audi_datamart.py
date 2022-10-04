from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil
from datetime import datetime, timedelta
import pandas as pd
from infra.spark_session import get_spark_session
from pyspark.sql.functions import *


class MovieAudi:
    movie_codes = []
    movie_names = []
    open_dates = []
    open_audi_cnts = []
    increase_ratios = []

    @classmethod
    def save(cls):
        movie_detail = find_data(DataWarehouse, 'MOVIE_DETAIL')
        movie_box_office = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        movie_hit = find_data(DataMart, 'MOVIE_HIT')

        movie_detail_open_audi_df = movie_detail.join(
            movie_box_office, on=['MOVIE_CODE', 'MOVIE_NAME'], how='left')
        open_audi_df = movie_detail_open_audi_df.select('MOVIE_CODE', 'MOVIE_NAME', movie_detail_open_audi_df.AUDI_CNT.alias('OPEN_AUDI_CNT'), to_date(movie_detail_open_audi_df.STD_DATE, 'yyyyMMdd').alias('STD_DATE'))\
            .where(movie_detail_open_audi_df.STD_DATE == '20220801')
        # .where(movie_detail_open_show.OPEN_DATE == movie_detail_open_show.STD_DATE)

        sec_date_df = open_audi_df.select('MOVIE_CODE', date_format(date_add(
            open_audi_df.STD_DATE, 7), 'yyyyMMdd').alias('SEC_DATE'))

        # 개봉일 2주차에 해당하는 관객 수 select
        sec_date_audi_df = sec_date_df.join(
            movie_box_office, on='MOVIE_CODE', how='left')
        sec_audi_df = sec_date_audi_df.select('MOVIE_CODE', sec_date_audi_df.AUDI_CNT.alias('SEC_AUDI_CNT'))\
            .where(sec_date_audi_df.STD_DATE == '20220802')
        # .where(sec_date_audi_df.STD_DATE == sec_date_audi_df.SEC_DATE)

        # (2주차 - 개봉일)/개봉일 : round(((sec_audi_cnt - open_audi_cnt) / open_audi_cnt * 100), 3)
        open_audi_df = open_audi_df.join(
            sec_audi_df, on='MOVIE_CODE', how='left')

        open_audi_df = open_audi_df.select('MOVIE_CODE', 'MOVIE_NAME', 'OPEN_AUDI_CNT', round(
            ((open_audi_df.SEC_AUDI_CNT - open_audi_df.OPEN_AUDI_CNT)/open_audi_df.OPEN_AUDI_CNT), 3).alias('SEC_AUDI_INCREASE'))

        hit_df = movie_hit.select('MOVIE_CODE', 'HIT_GRADE')

        movie_audi_df = open_audi_df.join(hit_df, on='MOVIE_CODE', how='left')
        save_data(DataMart, movie_audi_df, 'MOVIE_AUDI')
