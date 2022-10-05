import unittest
from datajob.datamart.actor_datamart import Actor
from datajob.datamart.movie_score_datamart import MovieScore
from datajob.datamart.movie_search_datamart import MovieSearch


class MTest(unittest.TestCase):
    def test1(self):
        MovieScore.save()

    def test2(self):
        Actor.save()

    def test3(self):
        MovieSearch.save()