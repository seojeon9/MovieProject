from pyspark.sql.functions import col
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType


class MovieDetailTrans:
    
    # MOVIECD = '20215601'

    @classmethod
    def transform(cls):
        box_office_df = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        movie_code_list = box_office_df.select('MOVIE_CODE').rdd.flatMap(lambda x: x).collect()
        movie_code_list = np.unique(movie_code_list)

        path = '/movie_data/detail/movie_detail_' + movie_code_list[0] + '.json'
        movie_json = get_spark_session().read.json(path, encoding='UTF-8')
        #tmp = movie_json.select('movieInfoResult.movieInfo').select('movieInfo.actors', 'movieInfo.audits', 'movieInfo.companys').first()
        #print(tmp)

        
        
        tmp = movie_json.select('movieInfoResult.movieInfo.movieCd', 'movieInfoResult.movieInfo.movieNm', 'movieInfoResult.movieInfo.prdtYear', 'movieInfoResult.movieInfo.showTm', 'movieInfoResult.movieInfo.openDt', 'movieInfoResult.movieInfo.typeNm', 'movieInfoResult.movieInfo.nations', 'movieInfoResult.movieInfo.directors', 'movieInfoResult.movieInfo.audits').first()
        print(tmp)
        #tmp = movie_json.select('movieInfoResult.movieInfo').first()

        #print(tmp)
        # schema = StructType([StructField("movieCd", StringType(), True),
        #                     StructField("movieNm", StringType(), True),
        #                     StructField("movieNmEn", StringType(), True),
        #                     StructField("movieNmOg", StringType(), True),
        #                     StructField("showTm", StringType(), True),
        #                     StructField("prdtYear", StringType(), True),
        #                     StructField("openDt", StringType(), True),
        #                     StructField("prdtStatNm", StringType(), True),
        #                     StructField("typeNm", StringType(), True),
        #                     StructField("nations", StructType([
        #                         StructField("nationNm", StringType(), True)
        #                     ]), True),
        #                     StructField("genres", StructType([
        #                         StructField("genreNm", StringType(), True)
        #                     ]), True),
        #                     StructField("directors", StructType([
        #                         StructField("peopleNm", StringType(), True),
        #                         StructField("peopleNmEn", StringType(), True)
        #                     ]), True),
        #                     StructField("actors", StructType([
        #                         StructField("peopleNm", StringType(), True),
        #                         StructField("peopleNmEn", StringType(), True),
        #                         StructField("cast", StringType(), True),
        #                         StructField("castEn", StringType(), True)
        #                     ]), True),
        #                     StructField("showTypes", StructType([
        #                         StructField("showTypeGroupNm", StringType(), True),
        #                         StructField("showTypeNm", StringType(), True)
        #                     ]), True),
        #                     StructField("companys", StructType([
        #                         StructField("companyCd", StringType(), True),
        #                         StructField("companyNm", StringType(), True),
        #                         StructField("companyNmEn", StringType(), True),
        #                         StructField("companyPartNm", StringType(), True)
        #                     ]), True),
        #                     StructField("audits", StructType([
        #                         StructField("auditNo", StringType(), True),
        #                         StructField("watchGradeNm", StringType(), True)
        #                     ]), True),
        #                     StructField("staffs", StructType([
        #                         StructField("peopleNm", StringType(), True),
        #                         StructField("peopleNmEn", StringType(), True),
        #                         StructField("staffRoleNm", StringType(), True)
        #                     ]), True)])
        df_movie = get_spark_session().createDataFrame([tmp])
        #df_movie.printSchema()
        df_movie.show()
        #df_movie_select = df_movie.select('movieCd', 'movieNm', 'prdtYear', 'showTm', 'openDt', 'typeNm', 'nations', 'directors', 'audits')
        
        # dump_df = df_movie_select.withColumn('nations', col('nations')[0].nationNm) \
        #                         .withColumn('directors', col('directors')[0].peopleNm) \
        #                         .withColumn('audits', col('audits')[0].watchGradeNm)
        # tmp_df = dump_df

        # for i in range(1, len(movie_code_list)):
        #     path = '/movie_data/detail/movie_detail_' + movie_code_list[i] + '.json'
        #     movie_json = get_spark_session().read.json(path, encoding='UTF-8')
        #     tmp = movie_json.select('movieInfoResult.movieInfo').first()
        #     df_movie = get_spark_session().createDataFrame(tmp)

        #     df_movie_select = df_movie.select('movieCd', 'movieNm', 'prdtYear', 'showTm', 'openDt', 'typeNm', 'nations', 'directors', 'audits')
            
        #     dump_df = df_movie_select.withColumn('nations', col('nations')[0].nationNm) \
        #                             .withColumn('directors', col('directors')[0].peopleNm) \
        #                             .withColumn('audits', col('audits')[0].watchGradeNm)

        #     tmp_df = tmp_df.union(dump_df).distinct()

        # movie_detail_df = tmp_df.withColumnRenamed('movieCd', 'MOVIE_CODE') \
        #                         .withColumnRenamed('movieNm', 'MOVIE_NAME') \
        #                         .withColumnRenamed('prdtYear', 'PRDT_YEAR') \
        #                         .withColumnRenamed('showTm', 'SHOW_TM') \
        #                         .withColumnRenamed('openDt', 'OPEN_DATE') \
        #                         .withColumnRenamed('typeNm', 'TYPE_NAME') \
        #                         .withColumnRenamed('nations', 'NATION_NAME') \
        #                         .withColumnRenamed('directors', 'DIRECTOR') \
        #                         .withColumnRenamed('audits', 'WATCH_GRADE_NAME')
        # movie_detail_df.show()

        # d = df_movie.select('genres')
        # d.show()
        # d.withColumn('genres', col('genres')[0].genreNm).show()

        # save_data(DataWarehouse, movie_detail_df, 'MOVIE_DETAIL')