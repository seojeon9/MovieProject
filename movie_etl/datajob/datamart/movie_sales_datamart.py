from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import when


class MovieSales:

    @classmethod
    def save(cls):
        movie_box_office = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        movie_accumulate = find_data(DataWarehouse, 'MOVIE_ACCUMULATE')
        movie_hit = find_data(DataMart, 'MOVIE_HIT')
        movie = find_data(DataMart, 'MOVIE')

        merge_df = movie_accumulate.join(movie_box_office, on='DBO_ID', how='left')

        select_df = merge_df.select('MOVIE_CODE', 'MOVIE_NAME','SALES_SHARE', 'SALES_ACC')

        select_open_sales_share_df = select_df.groupby(select_df.MOVIE_CODE, select_df.MOVIE_NAME).agg({'SALES_SHARE' :'min'})
        select_open_sales_share_df = select_open_sales_share_df.withColumnRenamed('min(SALES_SHARE)', 'OPEN_SALES_SHARE')

        select_avg_sales_share_df = select_df.groupby(select_df.MOVIE_CODE, select_df.MOVIE_NAME).agg({'SALES_SHARE' :'avg'})
        select_avg_sales_share_df = select_avg_sales_share_df.withColumnRenamed('avg(SALES_SHARE)', 'AVG_SALES_SHARE')
        
        select_max_df = select_df.groupby(select_df.MOVIE_CODE, select_df.MOVIE_NAME).agg({'SALES_ACC' :'max'})
        select_max_df = select_max_df.withColumnRenamed('max(SALES_ACC)', 'TOT_SALES')

        merge_open_avg_sales_share_df = select_open_sales_share_df.join(select_max_df, on=['MOVIE_CODE', 'MOVIE_NAME'], how='left')

        merge_open_avg_sales_share_tot_df = merge_open_avg_sales_share_df.join(select_avg_sales_share_df, on=['MOVIE_CODE', 'MOVIE_NAME'], how='left')

        hit_select_df = movie_hit.select('MOVIE_CODE', 'HIT_GRADE')

        movie_sales_df = merge_open_avg_sales_share_tot_df.join(hit_select_df, on='MOVIE_CODE', how='left')

        movie_select_df = movie.select('MOVIE_CODE', 'MOVIE_NAME')

        movie_sales_df = movie_sales_df.join(movie_select_df, on=['MOVIE_CODE', 'MOVIE_NAME'], how='leftsemi')
        movie_sales_df.show()

        save_data(DataMart, movie_sales_df, 'MOVIE_SALES')
