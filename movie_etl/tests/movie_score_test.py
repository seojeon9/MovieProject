import unittest

from datajob.etl.extract.movie_score import MovieScoreExtractor

class MTest(unittest.TestCase):
    # def test1(self):
    #     NaverSearchMovieExtractor.extract_data()

    def test1(self):
        MovieScoreExtractor.extract_data()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
