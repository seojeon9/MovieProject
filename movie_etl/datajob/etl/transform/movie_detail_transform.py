from pyspark.sql.functions import col
from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import get_spark_session


class MovieDetailTransformer:
    
    MOVIECD = '20215601'

    @classmethod
    def transform(cls):
        path = '/movie_data/detail/movie_detail_' + cls.MOVIECD + '.json'
        movie_json = get_spark_session().read.json(path, encoding='UTF-8')
        tmp = movie_json.select('movieInfoResult.movieInfo').first()
        df_movie = get_spark_session().createDataFrame(tmp)

        df_movie_select = df_movie.select('movieCd', 'movieNm', 'prdtYear', 'showTm', 'openDt', 'typeNm', 'nations', 'directors', 'audits')
        
        dump_df = df_movie_select.withColumn('nations', col('nations')[0].nationNm) \
                                 .withColumn('directors', col('directors')[0].peopleNm) \
                                 .withColumn('audits', col('audits')[0].watchGradeNm)

        movie_detail_df = dump_df.withColumnRenamed('movieCd', 'MOVIE_CODE') \
                                 .withColumnRenamed('movieNm', 'MOVIE_NAME') \
                                 .withColumnRenamed('prdtYear', 'PRDT_YEAR') \
                                 .withColumnRenamed('showTm', 'SHOW_TM') \
                                 .withColumnRenamed('openDt', 'OPEN_DATE') \
                                 .withColumnRenamed('typeNm', 'TYPE_NAME') \
                                 .withColumnRenamed('nations', 'NATION_NAME') \
                                 .withColumnRenamed('directors', 'DIRECTOR') \
                                 .withColumnRenamed('audits', 'WATCH_GRADE_NAME')
        movie_detail_df.show()

        df_movie.select('movieCd', 'genres').show()

        # save_data(DataWarehouse, movie_detail_df, 'MOVIE_DETAIL')
