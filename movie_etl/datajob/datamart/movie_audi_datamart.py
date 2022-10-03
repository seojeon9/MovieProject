from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil
from datetime import datetime, timedelta
import pandas as pd
from infra.spark_session import get_spark_session


class MovieAudi:
    movie_codes = []
    movie_names = []
    open_dates = []
    open_audi_cnts = []
    increase_ratios = []

    @classmethod
    def save(cls):
        movie_detail = find_data(DataWarehouse, 'MOVIE_DETAIL')
        DBO_table = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        movie_hit = find_data(DataMart, 'MOVIE_HIT')

        # print(type(data))  # pyspark.sql.dataframe.DataFrame
        movie_detail = movie_detail.select(
            'MOVIE_CODE', 'MOVIE_NAME', 'OPEN_DATE')
        movie_detail = movie_detail.to_pandas_on_spark()
        for dt in movie_detail.values:
            print(dt)
            cls.movie_codes.append(dt[0])
            cls.movie_names.append(dt[1])
            cls.open_dates.append(dt[2])

        for i in range(len(cls.movie_codes)):
            code = cls.movie_codes[i]
            opendate = cls.open_dates[i]

            secdate = timedelta(int(opendate)) + timedelta(7)
            secdate = str(secdate).split(' ')[0]

            taget_table = DBO_table.select('MOVIE_CODE', 'STD_DATE', 'AUDI_CNT') \
                .where((DBO_table.MOVIE_CODE == code))

            # 만약 우리가 수집 시작한 날보다 개봉일이 빠르다면 해당 영화는 수집 종료
            # 만약 1주일 후가 현재 날짜보다 늦다면 해당영화는 수집 종료
            # 근데 일단 지금은 수집된 날짜가 0801, 0802니까 그걸로 진행
            # open_audi = taget_table.select('AUDI_CNT') \
            #     .where(DBO_table.STD_DATE == opendate).collect()
            open_audi = taget_table.select('AUDI_CNT') \
                .where(DBO_table.STD_DATE == '20220801').collect()

            open_audi_cnt = open_audi[0]['AUDI_CNT']
            print(type(open_audi_cnt), open_audi_cnt)
            cls.open_audi_cnts.append(open_audi_cnt)

            # sec_audi = taget_table.select('AUDI_CNT') \
            #     .where(DBO_table.STD_DATE == secdate).collect()
            sec_audi = taget_table.select('AUDI_CNT') \
                .where(DBO_table.STD_DATE == '20220802').collect()

            sec_audi_cnt = sec_audi[0]['AUDI_CNT']
            print(type(sec_audi_cnt), sec_audi_cnt)

            increase_ratio = round(
                ((sec_audi_cnt - open_audi_cnt) / open_audi_cnt * 100), 3)
            print(increase_ratio)
            cls.increase_ratios.append(increase_ratio)

        MA_df = pd.DataFrame({
            'MOVIE_CODE': cls.movie_codes,
            'MOVIE_NAME': cls.movie_names,
            'OPEN_AUDI_CNT': cls.open_audi_cnts,
            'SEC_AUDI_INCREASE': cls.increase_ratios
        })

        MA_df = get_spark_session().createDataFrame(MA_df)

        hit_df = movie_hit.select('MOVIE_CODE', 'HIT_GRADE')

        movie_audi_df = MA_df.join(hit_df, on='MOVIE_CODE', how='left')
        save_data(DataMart, movie_audi_df, 'MOVIE_AUDI')
