import unittest
from datajob.etl.extract.naver_datalab_api import NaverDatalabApiExtractor
from datajob.etl.extract.naver_search_api import NaverSearchMovieExtractor


class MTest(unittest.TestCase):
    # def test1(self):
    #     NaverSearchMovieExtractor.extract_data()

    def test1(self):
        NaverDatalabApiExtractor.extract_data()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
