from infra.jdbc import DataMart, DataWarehouse, find_data, save_data

class MovieScore:
    @classmethod
    def save(cls):
        score = find_data(DataWarehouse, 'MOVIE_SCORE')
        hit = find_data(DataMart, 'MOVIE_HIT')

        score_join = score.join(hit, on='MOVIE_CODE')
        score_join = score_join.select('*').where(score_join.AUDI_SC!='없음').where(score_join.EXPE_SC!='없음').where(score_join.NETI_SC!='없음')
        movie_score = score_join.select(score_join.MOVIE_CODE
                                    ,score_join.MOVIE_NAME
                                    ,score_join.EXPE_SC.alias('EXPE_SCORE').cast('float')
                                    ,score_join.AUDI_SC.alias('AUDI_SCORE').cast('float')
                                    ,score_join.NETI_SC.alias('NETI_SCORE').cast('float')
                                    ,score_join.HIT_GRADE)

        save_data(DataMart, movie_score, 'MOVIE_SCORE')

        