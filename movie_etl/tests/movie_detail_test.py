import unittest
from datajob.etl.extract.movie_detail import MovieDetailApiExtractor
from datajob.etl.transform.movie_detail_transform import MovieDetailTransformer


class MTest(unittest.TestCase):
    def test1(self):
        MovieDetailApiExtractor.extract_data()

    def test2(self):
        MovieDetailTransformer.transform()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
