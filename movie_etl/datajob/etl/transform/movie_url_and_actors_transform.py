from pyspark.sql.functions import col
import pandas as pd
from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import get_spark_session
from infra.util import get_movie_codes


class MovieUrlAndActorsTransformer:

    @classmethod
    def transform(cls):
        movie_codes, movie_names = get_movie_codes()

        for movie_code in movie_codes:
            path = '/naver/search_movie/naver_search_movie_' + movie_code + '.json'
            movie = get_spark_session().read.option(
                "multiline", "true").json(path, encoding='UTF-8').first()

            items = get_spark_session().createDataFrame(movie['items'])
            items = items['actor', 'link', 'title'].first()
            print(items)

            actors = items.actor
            actors = actors.split('|')
            actors = actors[:-1]
            link = items.link

            movie_codes = []
            for ac in actors:
                movie_codes.append(movie_code)

            actors_df = pd.DataFrame({
                'MOVIE_CODE': movie_codes,
                'ACTOR_NAME': actors
            })
            actors_df = get_spark_session().createDataFrame(actors_df)

            link_df = pd.DataFrame({
                'MOVIE_CODE': [movie_code],
                'URL': [link]
            })
            link_df = get_spark_session().createDataFrame(link_df)

            # print(actors_df)
            save_data(DataWarehouse, actors_df, 'ACTORS')
            save_data(DataWarehouse, link_df, 'MOVIE_URL')
