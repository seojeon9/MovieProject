import unittest
from datajob.datamart.actor_datamart import Actor
from datajob.datamart.movie_score_datamart import MovieScore
from datajob.datamart.movie_search_datamart import MovieSearch
from datajob.etl.extract.movie_score import MovieScoreExtractor
from datajob.etl.transform.movie_score_transform import MovieScoreTransformer

class MTest(unittest.TestCase):
    def test1(self):
        MovieScoreExtractor.extract_data()
    
    def test2(self):
        MovieScoreTransformer.transform()

    def test3(self):
        MovieScore.save()

    def test4(self):
        Actor.save()
    
    def test5(self):
        MovieSearch.save()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
