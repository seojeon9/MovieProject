from pyspark.sql.functions import col, split
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day, cal_std_day_yyyymmdd


class DailyBoxOfficeTransformer:

    # date = '20220825'

    @classmethod
    def transform(cls, befor_cnt=1):
        date = cal_std_day_yyyymmdd(befor_cnt)

        df_movie_select = cls.__load_daily_box_office_json(befor_cnt)
            
        dump_df = cls.__select_columns(df_movie_select, 0)
        tmp_df = dump_df

        for i in range(1, 10):
            dump_df = cls.__select_columns(df_movie_select, i)
            tmp_df = tmp_df.union(dump_df)

        daily_boxoffice_df = cls.__rename_columns(tmp_df)
        
        save_data(DataWarehouse, daily_boxoffice_df, 'DAILY_BOXOFFICE')

        box_office_df = cls.load_box_office_table_dbo_id(date)
        tmp_df = cls.__select_acc(df_movie_select, 0)
        
        for i in range(1, 10):
            dump_df = cls.__select_acc(df_movie_select, i)
            tmp_df = tmp_df.union(dump_df)

        accumulate_df = cls.__generate_movie_accumulate(tmp_df, box_office_df)

        save_data(DataWarehouse, accumulate_df, 'MOVIE_ACCUMULATE')

    @classmethod
    def __generate_movie_accumulate(cls, tmp_df, box_office_df):
        merge_tmp = box_office_df.join(tmp_df, box_office_df.MOVIE_CODE == tmp_df.movieCd)
        merge_tmp = merge_tmp.drop(merge_tmp.MOVIE_CODE) \
                             .drop(merge_tmp.showRange) \
                             .drop(merge_tmp.dailyBoxOfficeList) \
                             .drop(merge_tmp.movieCd)
        accumulate_df = merge_tmp.withColumnRenamed('salesAcc', 'SALES_ACC') \
                                 .withColumnRenamed('audiAcc', 'AUDI_ACC')
                             
        return accumulate_df

    @classmethod
    def __select_acc(cls, df_movie_select, i):
        dump_df = df_movie_select.withColumn('movieCd', col('dailyBoxOfficeList')[i].movieCd) \
                                 .withColumn('salesAcc', col('dailyBoxOfficeList')[i].salesAcc) \
                                 .withColumn('audiAcc', col('dailyBoxOfficeList')[i].audiAcc)
                                 
        return dump_df

    @classmethod
    def load_box_office_table_dbo_id(cls, date):
        box_office_df = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        box_office_df = box_office_df.select('DBO_ID', 'MOVIE_CODE').where(box_office_df.STD_DATE == date)
        return box_office_df

    @classmethod
    def __rename_columns(cls, tmp_df):
        daily_boxoffice_df = tmp_df.withColumnRenamed('movieCd', 'MOVIE_CODE') \
                                   .withColumnRenamed('movieNm', 'MOVIE_NAME') \
                                   .withColumnRenamed('std_date', 'STD_DATE') \
                                   .withColumnRenamed('rank', 'RANK') \
                                   .withColumnRenamed('salesAmt', 'SALES_AMT') \
                                   .withColumnRenamed('salesShare', 'SALES_SHARE') \
                                   .withColumnRenamed('audiCnt', 'AUDI_CNT') \
                                   .withColumnRenamed('scrnCnt', 'SCRN_CNT') \
                                   .withColumnRenamed('showCnt', 'SHOW_CNT')
                                   
        return daily_boxoffice_df

    @classmethod
    def __select_columns(cls, df_movie_select, i):
        dump_df = df_movie_select.withColumn('movieCd', col('dailyBoxOfficeList')[i].movieCd) \
                                 .withColumn('movieNm', col('dailyBoxOfficeList')[i].movieNm) \
                                 .withColumn('rank', col('dailyBoxOfficeList')[i].rank) \
                                 .withColumn('salesAmt', col('dailyBoxOfficeList')[i].salesAmt) \
                                 .withColumn('salesShare', col('dailyBoxOfficeList')[i].salesShare) \
                                 .withColumn('audiCnt', col('dailyBoxOfficeList')[i].audiCnt) \
                                 .withColumn('scrnCnt', col('dailyBoxOfficeList')[i].scrnCnt) \
                                 .withColumn('showCnt', col('dailyBoxOfficeList')[i].showCnt) \
                                 .withColumn("std_date", split(col("showRange"), "~").getItem(0))
        dump_df = dump_df.drop(dump_df.dailyBoxOfficeList) \
                         .drop(dump_df.showRange)
                         
        return dump_df

    @classmethod
    def __load_daily_box_office_json(cls, befor_cnt=1):
        path = '/movie/daily_box_office/daliy_box_office_' + cal_std_day_yyyymmdd(befor_cnt) + '.json'
        #path = '/movie_data/daily_box_office/daliy_box_office_' + cls.date + '.json'
        movie_json = get_spark_session().read.json(path, encoding='UTF-8')

        tmp = movie_json.select('boxOfficeResult.showRange', 'boxOfficeResult.dailyBoxOfficeList').first()
        df_movie = get_spark_session().createDataFrame([tmp])

        df_movie_select = df_movie.select('showRange', 'dailyBoxOfficeList')
        return df_movie_select
