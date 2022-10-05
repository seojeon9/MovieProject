import unittest
from datajob.etl.extract.daily_boxoffice_api import DailyBoxofficeExtractor
from datajob.etl.transform.daily_boxoffice_transform import DailyBoxOfficeTransformer


class MTest(unittest.TestCase):
    # def test1(self):
    #     NaverSearchMovieExtractor.extract_data()

    def test1(self):
        DailyBoxofficeExtractor.extract_data(20220801, 20220831)
    
    def test2(self):
        DailyBoxOfficeTransformer.transform()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
