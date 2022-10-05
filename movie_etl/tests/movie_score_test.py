import unittest
from datajob.etl.extract.movie_score import MovieScoreExtractor
from datajob.etl.transform.movie_score_transform import MovieScoreTransformer

class MTest(unittest.TestCase):
    def test1(self):
        MovieScoreExtractor.extract_data()
    def test2(self):
        MovieScoreTransformer.transform()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
