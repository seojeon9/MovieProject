import unittest
from datajob.etl.extract.sample_api import SampleApiExtractor


class MTest(unittest.TestCase):
    def test1(self):
        SampleApiExtractor.transform()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
