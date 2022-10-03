import unittest

from datajob.datamart.movie_audi_datamart import MovieAudi
from datajob.datamart.movie_show_datamart import MovieShow
from datajob.datamart.movie_scrn_datamart import MovieScrn


class MTest(unittest.TestCase):
    def test1(self):
        MovieAudi.save()

    def test2(self):
        MovieShow.save()

    def test3(self):
        MovieScrn.save()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
