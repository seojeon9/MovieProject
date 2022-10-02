from pyspark.sql.functions import col
import pandas as pd
from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import get_spark_session


class MovieUrlAndActorsTransformer:

    @classmethod
    def transform(cls):
        path = '/naver/search_movie/naver_search_movie_20211472.json'
        movie = get_spark_session().read.option(
            "multiline", "true").json(path, encoding='UTF-8').first()

        items = get_spark_session().createDataFrame(movie['items'])
        items = items['actor', 'link', 'title'].first()

        actors = items.actor
        actors = actors.split('|')
        actors = actors[:-1]
        link = items.link

        movie_codes = []
        for ac in actors:
            movie_codes.append(('20211472'))

        actors_df = pd.DataFrame({
            'MOVIE_CODE': movie_codes,
            'ACTOR_NAME': actors
        })
        actors_df = get_spark_session().createDataFrame(actors_df)

        print(actors_df)
        save_data(DataWarehouse, actors_df, 'ACTORS')
