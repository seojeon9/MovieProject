from pyspark.sql.functions import col
from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import get_spark_session


class MovieSearchRatioTransfomer:

    @classmethod
    def transform(cls):
        path = '/naver/datalab/naver_datalab_20211472.json'
        dbo_json = get_spark_session().read.json(path, encoding='UTF-8').first()
        mm = get_spark_session().createDataFrame(dbo_json['results'])
        mmm = mm.first()
        # <class 'pyspark.sql.dataframe.DataFrame'>
        data = get_spark_session().createDataFrame(mmm['data'])
        # Can not infer schema for type: <class 'str'>
        title = get_spark_session().createDataFrame(mmm['title'])
        # tmp = dbo_json.select(
        #     'boxofficeType.dailyBoxOfficeList')

        # tmp.show()
        # df_movie = get_spark_session().createDataFrame(tmp)

        # df_movie_select = df_movie.select(
        #     'movieCd', 'movieNm', 'prdtYear', 'showTm', 'openDt', 'typeNm', 'nations', 'directors', 'audits')

        # dump_df = df_movie_select.withColumn('nations', col('nations')[0].nationNm) \
        #                          .withColumn('directors', col('directors')[0].peopleNm) \
        #                          .withColumn('audits', col('audits')[0].watchGradeNm)

        # movie_detail_df = dump_df.withColumnRenamed('movieCd', 'MOVIE_CODE') \
        #                          .withColumnRenamed('movieNm', 'MOVIE_NAME') \
        #                          .withColumnRenamed('prdtYear', 'PRDT_YEAR') \
        #                          .withColumnRenamed('showTm', 'SHOW_TM') \
        #                          .withColumnRenamed('openDt', 'OPEN_DATE') \
        #                          .withColumnRenamed('typeNm', 'TYPE_NAME') \
        #                          .withColumnRenamed('nations', 'NATION_NAME') \
        #                          .withColumnRenamed('directors', 'DIRECTOR') \
        #                          .withColumnRenamed('audits', 'WATCH_GRADE_NAME')
        # movie_detail_df.show()

        # df_movie.select('movieCd', 'genres').show()

        # save_data(DataWarehouse, movie_detail_df, 'MOVIE_DETAIL')
