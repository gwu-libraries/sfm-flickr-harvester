import flickrapi
import tests
import httplib as http_client
from flickr_harvester import FLICKR_HOST
from harvester.httplib_helper import wrap_api_call, parse_url
import unittest
import urlparse

@unittest.skipIf(tests.integration_env_available, "Skipping test since integration env is available.")
class HttpLibHelperTest(tests.TestCase):

    @unittest.skipIf(not tests.test_config_available, "Skipping test since test config not available.")
    def test_wrap_api_call(self):
        api = flickrapi.FlickrAPI(tests.FLICKR_KEY, tests.FLICKR_SECRET, store_token=False)
        request_record, response_record, raw_resp = wrap_api_call(
            lambda: api.photos.getInfo(photo_id="16796603565", secret="90f7d5c74c", format='json'),
            http_client.HTTPConnection,
            FLICKR_HOST
        )

        #Warc records
        self.assertEqual(request_record.type, "request")
        self.assertEqual(response_record.type, "response")

        #Raw resp
        self.assertTrue(raw_resp.startswith("""{"photo":{"id":"16796603565","secret":"90f7d5c74c","""))

        #Url
        self.assertTrue(response_record.header["WARC-Target-URI"].startswith("https://api.flickr.com/services/rest/?"))
        split_url = urlparse.urlparse(response_record.header["WARC-Target-URI"])
        self.assertDictEqual({"nojsoncallback": ["1"],
                              "secret": ["90f7d5c74c"],
                              "photo_id": ["16796603565"],
                              "method": ["flickr.photos.getInfo"],
                              "format": ["json"]}, urlparse.parse_qs(split_url.query))

    def test_parse_url(self):
        self.assertEqual("/services/rest/?user_id=131866249&method=flickr.people.getPublicPhotos",
                         parse_url("POST /services/rest/?user_id=131866249&method=flickr.people.getPublicPhotos "
                                   "HTTP/1.1"))

        self.assertEqual("/services/rest/?user_id=131866249&method=flickr.people.getPublicPhotos",
                         parse_url("GET /services/rest/?user_id=131866249&method=flickr.people.getPublicPhotos "
                                   "HTTP/1.1"))
