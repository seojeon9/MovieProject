from pyspark.sql.functions import col
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
import pandas as pd

class MovieScoreTransformer:
    @classmethod
    def transform(cls):
        movie_url_df = find_data(DataWarehouse, 'MOVIE_URL')
        movie_url_df = movie_url_df.drop_duplicates(['MOVIE_CODE'])
        dw_movie_score_df = find_data(DataWarehouse, 'MOVIE_SCORE').drop('MS_ID')
        movie_code_list = movie_url_df.select('MOVIE_CODE').rdd.flatMap(lambda x: x).collect()

        audi_score_list = []
        expe_score_list = []
        neti_score_list =[]
        std_date_list = []

        for i in range(len(movie_code_list)):
            path = '/movie_data/score/movie_score_' + \
                movie_code_list[i] + '_' + \
                cal_std_day(1) + '.json'
            mv_score_json = get_spark_session().read.json(path, encoding='UTF-8').first()
            mv_score_row = get_spark_session().createDataFrame(mv_score_json['data']).first()
            
            audi_score_list.append(mv_score_row.audi_sc)
            expe_score_list.append(mv_score_row.expe_sc)
            neti_score_list.append(mv_score_row.neti_sc)
            std_date_list.append(mv_score_row.std_date)
            

        movie_score = pd.DataFrame({
            'MOVIE_CODE':movie_code_list,
            'AUDI_SC':audi_score_list,
            'EXPE_SC':expe_score_list,
            'NETI_SC':neti_score_list,
            'STD_DATE':std_date_list
        })

        movie_score_df = get_spark_session().createDataFrame(movie_score)
        movie_score_sub = dw_movie_score_df.subtract(movie_score_df)
        
        save_data(DataWarehouse, movie_score_sub, 'MOVIE_SCORE')
