from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import when


class Movie:

    @classmethod
    def save(cls):
        movie_box_office, movie_detail, movie_company, movie_hit, movie_genre, genre = cls.__load_data()
        select_df = cls.__merge_detail_box_office(movie_box_office, movie_detail)
        add_peak_df = cls.__add_peak_column(select_df)
        merge_hit_df = cls.__add_hit_column(movie_hit, add_peak_df)
        add_company = cls.__add_company_column(movie_company, merge_hit_df)
        merge_genre = cls.generate_genre_column(movie_genre, genre)
        movie_df = cls.__generate_movie_df(add_company, merge_genre)
        movie_df.show()

        save_data(DataMart, movie_df, 'MOVIE')

    @classmethod
    def __generate_movie_df(cls, add_company, merge_genre):
        movie_df = add_company.join(merge_genre, on='MOVIE_CODE', how='left')

        return movie_df

    @classmethod
    def generate_genre_column(cls, movie_genre, genre):
        select_min_df = movie_genre.groupby(movie_genre.MOVIE_CODE).agg({'MG_ID' :'min'})
        select_min_df = select_min_df.withColumnRenamed('min(MG_ID)', 'MG_ID')
        
        select_min_df = select_min_df.join(movie_genre, on=['MOVIE_CODE', 'MG_ID'], how='left')
        select_min_df = select_min_df.drop(select_min_df.MG_ID)

        merge_genre = select_min_df.join(genre, on='GENRE_ID', how='left')
        merge_genre = merge_genre.drop(merge_genre.GENRE_ID)

        return merge_genre

    @classmethod
    def __add_company_column(cls, movie_company, merge_hit_df):
        movie_company = movie_company.select('*').where(movie_company.COMPANY_PART_NAME == '배급사')
        select_min_df = movie_company.groupby(movie_company.MOVIE_CODE).agg({'MC_ID' :'min'})
        select_min_df = select_min_df.withColumnRenamed('min(MC_ID)', 'MC_ID')
        select_min_df = select_min_df.join(movie_company, on=['MOVIE_CODE', 'MC_ID'], how='left')
        select_min_df = select_min_df.select('MOVIE_CODE', 'COMPANY_NAME')

        add_company = merge_hit_df.join(select_min_df, on='MOVIE_CODE', how='left')
        add_company = add_company.withColumnRenamed('COMPANY_NAME', 'DIST_NAME')

        return add_company

    @classmethod
    def __add_hit_column(cls, movie_hit, add_peak_df):
        merge_hit_df = add_peak_df.join(movie_hit, on=['MOVIE_CODE', 'MOVIE_NAME'], how='left')
        merge_hit_df = merge_hit_df.drop(merge_hit_df.TOT_AUDI_CNT)
        return merge_hit_df

    @classmethod
    def __add_peak_column(cls, select_df):
        add_peak_df = select_df.withColumn('PEAK_YN', when((select_df.OPEN_DATE.like("____%01%")) | (select_df.OPEN_DATE.like("____%02%")) | (select_df.OPEN_DATE.like("____%07%") | (select_df.OPEN_DATE.like("____%08%")) | (select_df.OPEN_DATE.like("____%12%"))), 'Y')
                                                     .otherwise('N'))
                                                     
        return add_peak_df

    @classmethod
    def __merge_detail_box_office(cls, movie_box_office, movie_detail):
        merge_detail_box = movie_detail.join(movie_box_office, on=['MOVIE_CODE', 'MOVIE_NAME'], how='left')
        select_df = merge_detail_box.select('MOVIE_CODE', 'MOVIE_NAME','SHOW_TM', 'OPEN_DATE', 'TYPE_NAME', 'NATION_NAME', 'DIRECTOR', 'WATCH_GRADE_NAME').distinct()
        
        return select_df

    @classmethod
    def __load_data(cls):
        movie_box_office = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        movie_detail = find_data(DataWarehouse, 'MOVIE_DETAIL')
        movie_company = find_data(DataWarehouse, 'MOVIE_COMPANY')
        movie_hit = find_data(DataMart, 'MOVIE_HIT')
        movie_genre = find_data(DataWarehouse, 'MOVIE_GENRE')
        genre = find_data(DataWarehouse, 'GENRE')

        return movie_box_office, movie_detail, movie_company, movie_hit, movie_genre, genre
