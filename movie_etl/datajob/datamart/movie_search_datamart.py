from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import *

class MovieSearch:
    @classmethod
    def save(cls):
        search = find_data(DataWarehouse, 'MOVIE_SEARCH_RATIO')
        movie = find_data(DataMart, 'MOVIE')
        search = search.select('MOVIE_CODE','RATIO', 'WEEK')
        movie = movie.select('MOVIE_CODE','MOVIE_NAME','HIT_GRADE')
        
        search_join = search.join(movie, on='MOVIE_CODE')
        first = search_join.select(search_join.MOVIE_CODE,search_join.RATIO.alias('FIRST_SEARCH_RATIO')).where(search_join.WEEK==1)
        second = search_join.select(search_join.MOVIE_CODE,search_join.RATIO.alias('SECOND_SEARCH_RATIO')).where(search_join.WEEK==2)
        third = search_join.select(search_join.MOVIE_CODE,search_join.RATIO.alias('THIRD_SEARCH_RATIO')).where(search_join.WEEK==3)

        ratio_join = first.join(second, on='MOVIE_CODE',how='left').join(third, on='MOVIE_CODE',how='left')
        movie_search = movie.join(ratio_join, on='MOVIE_CODE', how='left')
        
        save_data(DataMart, movie_search, 'MOVIE_SEARCH')