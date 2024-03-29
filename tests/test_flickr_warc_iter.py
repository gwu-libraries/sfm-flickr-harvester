from __future__ import absolute_import
import tests
from flickr_warc_iter import FlickrWarcIter, TYPE_FLICKR_PHOTO


class TestFlickrWarcIter(tests.TestCase):
    def setUp(self):
        self.filepaths = ("tests/warcs/1/2016/02/22/14/612bc180ec8346c4bbc5aa2c52dcc952-20160222143621927-00000-37"
                          "-551fcc0ef48b-8000.warc.gz")

    def test_no_limit(self):
        warc_iter = FlickrWarcIter(self.filepaths)
        photos = list(warc_iter.iter(limit_item_types=[TYPE_FLICKR_PHOTO]))
        self.assertEqual(12, len(photos))
        self.assertEqual("flickr_photo", photos[0][0])
        self.assertEqual("16610484049", photos[0][1])
        # Datetime is aware
        self.assertIsNotNone(photos[0][2].tzinfo)
