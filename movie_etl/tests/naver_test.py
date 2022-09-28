import unittest
from datajob.etl.extract.naver_movie_api import NaverSearchMovieExtractor


class MTest(unittest.TestCase):
    def test1(self):
        NaverSearchMovieExtractor.extract_data()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
