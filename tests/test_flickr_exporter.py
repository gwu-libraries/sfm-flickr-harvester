import tests
import vcr as base_vcr
from flickr_exporter import FlickrExporter, FlickrPhotoTable
import os
import tempfile
import shutil

vcr = base_vcr.VCR(
    cassette_library_dir='tests/fixtures',
    record_mode='once',
)


# Notes:
# To find the number of photos in a WARC: zgrep "WARC-Target-URI:.*method=flickr.photos.getInfo" the_warc.warc.gz -c
# and divide by 2.
# d8ecf0efa0fa49819f907930bb766f69-20160222143902368-00000-70-551fcc0ef48b-8000.warc.gz ran to slow, so warc records
# were removed. The original file is prefixed with orig_.


class TestFlickrExporter(tests.TestCase):
    def setUp(self):
        self.warc_base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "warcs")
        self.working_path = tempfile.mkdtemp()
        self.exporter = FlickrExporter("http://192.168.99.100:8081/", self.working_path,
                                       warc_base_path=self.warc_base_path)
        self.exporter.routing_key = "export.request.flickr.flickr_user"
        self.export_path = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.exists(self.export_path):
            shutil.rmtree(self.export_path)
        if os.path.exists(self.working_path):
            shutil.rmtree(self.working_path)

    @vcr.use_cassette()
    def test_export_collection(self):

        export_message = {
            "id": "test1",
            "type": "flickr_user",
            "collection": {
                "id": "005b131f5f854402afa2b08a4b7ba960"
            },
            "format": "csv",
            "path": self.export_path
        }

        self.exporter.message = export_message
        self.exporter.on_message()

        self.assertTrue(self.exporter.result.success)
        csv_filepath = os.path.join(self.export_path, "test1.csv")
        self.assertTrue(os.path.exists(csv_filepath))
        with open(csv_filepath, "r") as f:
            lines = f.readlines()
        self.assertEqual(33, len(lines))

    @vcr.use_cassette()
    def test_export_seeds(self):
        export_message = {
            "id": "test5",
            "type": "flickr_user",
            "seeds": [
                {
                    "id": "48722ac6154241f592fd74da775b7ab7",
                    "uid": "23972344@N05"
                },
                {
                    "id": "3ce76759a3ee40b894562a35359dfa54",
                    "uid": "85779209@N08"
                }
            ],
            "format": "csv",
            "path": self.export_path
        }
        self.exporter.message = export_message
        self.exporter.on_message()

        self.assertTrue(self.exporter.result.success)
        csv_filepath = os.path.join(self.export_path, "test5.csv")
        self.assertTrue(os.path.exists(csv_filepath))
        with open(csv_filepath, "r") as f:
            lines = f.readlines()
        self.assertEqual(50, len(lines))


class TestFlickrPhotoTable(tests.TestCase):
    def setUp(self):
        warc_base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "warcs/1/2016/02/22")
        self.warc_paths = (os.path.join(warc_base_path,
                                        "14/612bc180ec8346c4bbc5aa2c52dcc952-20160222143621927-00000-37-551fcc0ef48b" +
                                        "-8000.warc.gz"),
                           os.path.join(warc_base_path,
                                        "15/c4ec6bc7112f4696b1f9372100ab93bb-20160222154659265-00000-286-551fcc0ef48b" +
                                        "-8000.warc.gz"))

    def test_table(self):
        table = FlickrPhotoTable(self.warc_paths, False, None, None, None)
        count = 0
        for count, row in enumerate(table):
            if count == 0:
                # Header row
                # Just testing first and last, figuring these might change often.
                self.assertEqual("photo_id", row[0])
                self.assertEqual("photopage", row[-1])
            if count == 1:
                # First row
                self.assertEqual("16610484049", row[0])
                self.assertEqual("https://www.flickr.com/photos/131866249@N02/16610484049/", row[-1])
        self.assertEqual(41, count)
