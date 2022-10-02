import unittest
from datajob.etl.extract.naver_datalab_api import NaverDatalabApiExtractor
from datajob.etl.extract.naver_search_api import NaverSearchMovieExtractor
from datajob.etl.transform.movie_url_and_actors_transform import MovieUrlAndActorsTransformer


class MTest(unittest.TestCase):
    def test1(self):
        NaverSearchMovieExtractor.extract_data()

    def test2(self):
        NaverDatalabApiExtractor.extract_data()

    def test3(self):
        MovieUrlAndActorsTransformer.transform()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
