from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from infra.logger import get_logger

class Actor:
    @classmethod
    def save(cls):
        actors = find_data(DataWarehouse, 'ACTORS')
        movie = find_data(DataMart, 'MOVIE')

        actors_join = actors.join(movie, on='MOVIE_CODE')
        actors_join.show()

        actor = actors_join.select(actors_join.MOVIE_CODE
                                    ,actors_join.MOVIE_NAME
                                    ,actors_join.ACTOR_NAME
                                    ,actors_join.HIT_GRADE)

        try:
            save_data(DataMart, actor, 'ACTOR')
        except:
            get_logger('dm actor').error(actor)