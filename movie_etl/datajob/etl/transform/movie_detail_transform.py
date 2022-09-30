from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
import numpy as np
from pyspark.sql.types import *
from pyspark.sql import Row


class MovieDetailTransformer:
    
    genre_movie_code_list = []
    genre_list = []

    company_movie_code_list = []
    company_code_list = []
    company_name_list = []
    company_part_list = []

    @classmethod
    def transform(cls):
        box_office_movie_code_list = cls.load_box_office_movie_code()

        past_movie_detail_df = find_data(DataWarehouse, 'MOVIE_DETAIL')
        movie_detail_code_list = past_movie_detail_df.select('MOVIE_CODE').rdd.flatMap(lambda x: x).collect()
        movie_code_list = list(set(box_office_movie_code_list) - set(movie_detail_code_list))

        df_movie = cls.load_movie_detail_json(movie_code_list, 0)

        cls.make_genre_list(df_movie, cls.genre_list, movie_code_list, 0)
        cls.make_company_code_list(df_movie, movie_code_list, 0)

        dump_df = cls.select_columns(df_movie)
        tmp_df = dump_df

        for i in range(1, len(movie_code_list)):
            df_movie = cls.load_movie_detail_json(movie_code_list, i)

            cls.make_genre_list(df_movie, cls.genre_list, movie_code_list, i)
            cls.make_company_code_list(df_movie, movie_code_list, i)

            dump_df = cls.select_columns(df_movie)

            tmp_df = tmp_df.union(dump_df).distinct()

        movie_detail_df = cls.rename_columns(tmp_df)

        movie_detail_df.show()
        
        save_data(DataWarehouse, movie_detail_df, 'MOVIE_DETAIL')

# ----------------------- 장르 테이블 Transform  -------------------------------
        rows = []
        for g in cls.genre_list:
            rows.append(Row(GENRE=g))
        genre_df = get_spark_session().createDataFrame(rows)

        rows2 = []
        for c in cls.genre_movie_code_list:
            rows2.append(Row(MOVIE_CODE=c))
        genre_movie_code_df = get_spark_session().createDataFrame(rows2)
        
        genre_movie_code_df = genre_movie_code_df.withColumn('idx', monotonically_increasing_id())
        genre_df = genre_df.withColumn('idx', monotonically_increasing_id())
        genre_mrege_movie_code_df = genre_movie_code_df.join(genre_df, on='idx')
        genre_mrege_movie_code_df = genre_mrege_movie_code_df.drop(genre_mrege_movie_code_df.idx)

        genre_table_df = find_data(DataWarehouse, 'GENRE')
        
        movie_genre_df = genre_mrege_movie_code_df.join(genre_table_df, on='GENRE', how='left')
        movie_genre_df = movie_genre_df.drop(movie_genre_df.GENRE)
        movie_genre_df.show()

        save_data(DataWarehouse, movie_genre_df, 'MOVIE_GENRE')
# ------------------------ company 테이블 Transform ------------------------------------
        rows = []
        for g in cls.company_code_list:
            rows.append(Row(COMPANY_CODE=g))
        company_code_df = get_spark_session().createDataFrame(rows)

        rows = []
        for g in cls.company_name_list:
            rows.append(Row(COMPANY_NAME=g))
        company_name_df = get_spark_session().createDataFrame(rows)

        rows = []
        for g in cls.company_part_list:
            rows.append(Row(COMPANY_PART_NAME=g))
        company_part_df = get_spark_session().createDataFrame(rows)

        rows = []
        for g in cls.company_movie_code_list:
            rows.append(Row(MOVIE_CODE=g))
        company_movie_code_df = get_spark_session().createDataFrame(rows)

        company_movie_code_df = company_movie_code_df.withColumn('idx', monotonically_increasing_id())
        company_code_df = company_code_df.withColumn('idx', monotonically_increasing_id())
        company_name_df = company_name_df.withColumn('idx', monotonically_increasing_id())
        company_part_df = company_part_df.withColumn('idx', monotonically_increasing_id())
        merge_tmp = company_movie_code_df.join(company_code_df, on='idx')
        merge_tmp = merge_tmp.join(company_name_df, on='idx')
        merge_tmp = merge_tmp.join(company_part_df, on='idx')
        movie_company_df = merge_tmp.drop(merge_tmp.idx)
        movie_company_df.show()

        save_data(DataWarehouse, movie_company_df, 'MOVIE_COMPANY')


    @classmethod
    def make_genre_list(cls, df_movie, genre_list, movie_code_list, n):
        genre_tmp = df_movie.select('genres').rdd.flatMap(lambda x: x).collect()[0]
        for i in range(len(genre_tmp)):
            genre_list.append(genre_tmp[i].genreNm)
            cls.genre_movie_code_list.append(str(movie_code_list[n]))

    @classmethod
    def make_company_code_list(cls, df_movie, movie_code_list, n):
        company_tmp = df_movie.select('companys').rdd.flatMap(lambda x: x).collect()[0]
        for i in range(len(company_tmp)):
            cls.company_code_list.append(company_tmp[i].companyCd)
            cls.company_name_list.append(company_tmp[i].companyNm)
            cls.company_part_list.append(company_tmp[i].companyPartNm)
            cls.company_movie_code_list.append(str(movie_code_list[n]))

    @classmethod
    def rename_columns(cls, tmp_df):
        movie_detail_df = tmp_df.withColumnRenamed('movieCd', 'MOVIE_CODE') \
                                .withColumnRenamed('movieNm', 'MOVIE_NAME') \
                                .withColumnRenamed('prdtYear', 'PRDT_YEAR') \
                                .withColumnRenamed('showTm', 'SHOW_TM') \
                                .withColumnRenamed('openDt', 'OPEN_DATE') \
                                .withColumnRenamed('typeNm', 'TYPE_NAME') \
                                .withColumnRenamed('nations', 'NATION_NAME') \
                                .withColumnRenamed('directors', 'DIRECTOR') \
                                .withColumnRenamed('audits', 'WATCH_GRADE_NAME')
        return movie_detail_df

    @classmethod
    def select_columns(cls, df_movie):
        df_movie_select = df_movie.select('movieCd', 'movieNm', 'prdtYear', 'showTm', 'openDt', 'typeNm', 'nations', 'directors', 'audits')
            
        dump_df = df_movie_select.withColumn('nations', col('nations')[0].nationNm) \
                                 .withColumn('directors', col('directors')[0].peopleNm) \
                                 .withColumn('audits', col('audits')[0].watchGradeNm)          
        return dump_df

    @classmethod
    def load_movie_detail_json(cls, movie_code_list, i):
        path = '/movie_data/detail/movie_detail_' + movie_code_list[i] + '.json'
        movie_json = get_spark_session().read.json(path, encoding='UTF-8')
        tmp = movie_json.select('movieInfoResult.movieInfo.movieCd', 'movieInfoResult.movieInfo.movieNm', 'movieInfoResult.movieInfo.prdtYear', 'movieInfoResult.movieInfo.showTm', 'movieInfoResult.movieInfo.openDt', 'movieInfoResult.movieInfo.typeNm', 'movieInfoResult.movieInfo.nations', 'movieInfoResult.movieInfo.directors', 'movieInfoResult.movieInfo.audits', 'movieInfoResult.movieInfo.genres', 'movieInfoResult.movieInfo.actors', 'movieInfoResult.movieInfo.companys').first()
        df_movie = get_spark_session().createDataFrame([tmp])
        return df_movie

    @classmethod
    def load_box_office_movie_code(cls):
        box_office_df = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        box_office_movie_code_list = box_office_df.select('MOVIE_CODE').rdd.flatMap(lambda x: x).collect()
        box_office_movie_code_list = np.unique(box_office_movie_code_list)
        return box_office_movie_code_list
