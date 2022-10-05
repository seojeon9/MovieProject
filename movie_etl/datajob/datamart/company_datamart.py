from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil


class Company:
    @classmethod
    def save(cls):
        movie_detail = find_data(DataWarehouse,'MOVIE_DETAIL')
        company  = find_data(DataWarehouse,'MOVIE_COMPANY')
        movie = find_data(DataMart, 'MOVIE')
        movie = movie.select('MOVIE_CODE','HIT_GRADE')

        company_join = movie_detail.join(company, on='MOVIE_CODE')
        company_join_df = company_join.join(movie, on='MOVIE_CODE', how='left')
        

        company = company_join_df.select(company_join_df.MOVIE_CODE
                                    ,company_join_df.COMPANY_NAME
                                    ,company_join_df.COMPANY_PART_NAME.alias('COMPANY_TYPE')
                                    ,company_join_df.HIT_GRADE)
        company.show()
        save_data(DataMart,company,'COMPANY')
