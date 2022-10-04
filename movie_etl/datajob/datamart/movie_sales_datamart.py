from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import when


class MovieSales:

    @classmethod
    def save(cls):
        movie_box_office, movie_accumulate, movie_hit, movie = cls.__load_data()
        select_df = cls.__merge_accumulate_box_df(movie_box_office, movie_accumulate)
        select_open_sales_share_df = cls.__generate_open_sales_share_df(select_df)
        select_avg_sales_share_df = cls.__generate_avg_sales_share_df(select_df)
        select_max_df = cls.__generate_tot_sales_df(select_df)
        movie_sales_df = cls.__generate_movie_sales_df(movie_hit, movie, select_open_sales_share_df, select_avg_sales_share_df, select_max_df)
        movie_sales_df.show()

        save_data(DataMart, movie_sales_df, 'MOVIE_SALES')

    @classmethod
    def __generate_movie_sales_df(cls, movie_hit, movie, select_open_sales_share_df, select_avg_sales_share_df, select_max_df):
        merge_open_avg_sales_share_df = select_open_sales_share_df.join(select_max_df, on=['MOVIE_CODE', 'MOVIE_NAME'], how='left')
        merge_open_avg_sales_share_tot_df = merge_open_avg_sales_share_df.join(select_avg_sales_share_df, on=['MOVIE_CODE', 'MOVIE_NAME'], how='left')
        hit_select_df = movie_hit.select('MOVIE_CODE', 'HIT_GRADE')
        movie_sales_df = merge_open_avg_sales_share_tot_df.join(hit_select_df, on='MOVIE_CODE', how='left')
        movie_select_df = movie.select('MOVIE_CODE', 'MOVIE_NAME')
        movie_sales_df = movie_sales_df.join(movie_select_df, on=['MOVIE_CODE', 'MOVIE_NAME'], how='leftsemi')

        return movie_sales_df

    @classmethod
    def __generate_tot_sales_df(cls, select_df):
        select_max_df = select_df.groupby(select_df.MOVIE_CODE, select_df.MOVIE_NAME).agg({'SALES_ACC' :'max'})
        select_max_df = select_max_df.withColumnRenamed('max(SALES_ACC)', 'TOT_SALES')

        return select_max_df

    @classmethod
    def __generate_avg_sales_share_df(cls, select_df):
        select_avg_sales_share_df = select_df.groupby(select_df.MOVIE_CODE, select_df.MOVIE_NAME).agg({'SALES_SHARE' :'avg'})
        select_avg_sales_share_df = select_avg_sales_share_df.withColumnRenamed('avg(SALES_SHARE)', 'AVG_SALES_SHARE')

        return select_avg_sales_share_df

    @classmethod
    def __generate_open_sales_share_df(cls, select_df):
        select_open_sales_share_df = select_df.groupby(select_df.MOVIE_CODE, select_df.MOVIE_NAME).agg({'SALES_SHARE' :'min'})
        select_open_sales_share_df = select_open_sales_share_df.withColumnRenamed('min(SALES_SHARE)', 'OPEN_SALES_SHARE')

        return select_open_sales_share_df

    @classmethod
    def __merge_accumulate_box_df(cls, movie_box_office, movie_accumulate):
        merge_df = movie_accumulate.join(movie_box_office, on='DBO_ID', how='left')
        select_df = merge_df.select('MOVIE_CODE', 'MOVIE_NAME','SALES_SHARE', 'SALES_ACC')

        return select_df

    @classmethod
    def __load_data(cls):
        movie_box_office = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        movie_accumulate = find_data(DataWarehouse, 'MOVIE_ACCUMULATE')
        movie_hit = find_data(DataMart, 'MOVIE_HIT')
        movie = find_data(DataMart, 'MOVIE')
        
        return movie_box_office, movie_accumulate, movie_hit, movie
