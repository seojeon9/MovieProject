from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import when


class MovieHit:

    @classmethod
    def save(cls):
        movie_box_office, movie_accumulate = cls.__load_data()
        movie_hit_df = cls.__generate_movie_hit_df(movie_box_office, movie_accumulate)
        movie_hit_df.show()

        save_data(DataMart, movie_hit_df, 'MOVIE_HIT')

    @classmethod
    def __generate_movie_hit_df(cls, movie_box_office, movie_accumulate):
        merge_df = movie_accumulate.join(movie_box_office, on='DBO_ID', how='left')

        select_df = merge_df.select('MOVIE_CODE', 'MOVIE_NAME','AUDI_ACC')
        select_max_df = select_df.groupby(select_df.MOVIE_CODE, select_df.MOVIE_NAME).agg({'AUDI_ACC' :'max'})
        select_max_df = select_max_df.withColumnRenamed('max(AUDI_ACC)', 'TOT_AUDI_CNT')

        cdf = select_max_df.withColumn('HIT_GRADE', when(select_max_df.TOT_AUDI_CNT > 7000000, 'S')
                                                   .when(select_max_df.TOT_AUDI_CNT > 4000000, 'A')
                                                   .when(select_max_df.TOT_AUDI_CNT > 1500000, 'C')
                                                   .when(select_max_df.TOT_AUDI_CNT > 400000, 'D')
                                                   .when(select_max_df.TOT_AUDI_CNT > 150000, 'E')
                                                   .otherwise('F'))

        movie_hit_df = cdf.select(cdf.MOVIE_CODE, cdf.MOVIE_NAME, cdf.TOT_AUDI_CNT, cdf.HIT_GRADE)

        return movie_hit_df

    @classmethod
    def __load_data(cls):
        movie_box_office = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        movie_accumulate = find_data(DataWarehouse, 'MOVIE_ACCUMULATE')

        return movie_box_office, movie_accumulate
