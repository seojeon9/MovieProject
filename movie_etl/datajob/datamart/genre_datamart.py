from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil


class Genre:
    @classmethod
    def save(cls):
        movie_detail  = find_data(DataWarehouse,'MOVIE_DETAIL')
        movie_genre = find_data(DataWarehouse,'MOVIE_GENRE')
        genre = find_data(DataWarehouse,'GENRE')
        movie = find_data(DataMart, 'MOVIE')
        movie = movie.select('MOVIE_CODE','HIT_GRADE')

        genre_join = movie_detail.join(movie_genre, on='MOVIE_CODE', how='left')
        movie_genre_join = genre_join.join(genre, on='GENRE_ID', how='left')
        movie_genre_join_df = movie_genre_join.join(movie, on='MOVIE_CODE',how='left')
        movie_genre_join_df.show()

        genre = movie_genre_join_df.select(movie_genre_join_df.MOVIE_CODE
                                        ,movie_genre_join_df.GENRE.alias('GENRE_NAME')
                                        ,movie_genre_join_df.HIT_GRADE)
        # movie_genre_join_df.show()

        save_data(DataMart,genre,'GENRE')