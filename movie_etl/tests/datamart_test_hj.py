import unittest
from datajob.datamart.movie_datamart import Movie
from datajob.datamart.movie_hit_datamart import MovieHit
from datajob.datamart.movie_sales_datamart import MovieSales


class MTest(unittest.TestCase):
    def test1(self):
        MovieHit.save()

    def test2(self):
        Movie.save()

    def test3(self):
        MovieSales.save()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
