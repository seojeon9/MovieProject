from pyspark.sql.functions import col
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day

class MovieScoreTransformer:
    @classmethod
    def transform(cls):
        movie_url_df = find_data(DataWarehouse, 'MOVIE_URL')
        movie_url_df = movie_url_df.drop_duplicates(['MOVIE_CODE'])
        movie_code_list = movie_url_df.select('MOVIE_CODE').rdd.flatMap(lambda x: x).collect()
        
        for i in range(len(movie_code_list)):
            try :
                path = '/movie_data/score/movie_score_' + \
                    movie_code_list[i] + '_' + \
                    cal_std_day(0) + '.json'
                mv_score_json = get_spark_session().read.json(path, encoding='UTF-8')
                tmp = mv_score_json.select('data').first()
                df_mv_score = get_spark_session().createDataFrame(tmp)
                df_mv_score = df_mv_score.select('_1').first()
                df_mv_score = get_spark_session().createDataFrame(df_mv_score)

                mv_score = df_mv_score.select(df_mv_score.movie_code.alias('MOVIE_CODE')
                                                ,df_mv_score.audi_sc.alias('AUDI_SC')
                                                ,df_mv_score.expe_sc.alias('EXPE_SC')
                                                ,df_mv_score.neti_sc.alias('NETI_SC')
                                                ,df_mv_score.std_date.alias('STD_DATE'))

                save_data(DataWarehouse, mv_score, 'MOVIE_SCORE')
            except :
                pass