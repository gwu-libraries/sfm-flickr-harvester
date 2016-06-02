import tests
import vcr as base_vcr
from flickr_harvester import FlickrHarvester
from sfmutils.state_store import DictHarvestStateStore
from sfmutils.harvester import HarvestResult, CODE_TOKEN_NOT_FOUND, CODE_UID_NOT_FOUND, EXCHANGE
import threading
from mock import patch, call
import unittest
import time
from kombu import Connection, Exchange, Queue, Producer
import shutil
import tempfile
from datetime import datetime, date


vcr = base_vcr.VCR(
        cassette_library_dir='tests/fixtures',
        record_mode='once',
    )


class TestFlickrHarvester(tests.TestCase):
    def setUp(self):
        self.harvester = FlickrHarvester(per_page=6)
        self.harvester.state_store = DictHarvestStateStore()
        # self.harvester.message = base_message
        self.harvester.harvest_result = HarvestResult()
        self.harvester.stop_event = threading.Event()
        self.harvester.harvest_result_lock = threading.Lock()
        self.harvester.message = {
            "id": "test:1",
            "type": "flickr_user",
            "path": "/collections/test_collection_set/collection_id",
            "seeds": [],
            "credentials": {
                "key": tests.FLICKR_KEY or "fake key",
                "secret": tests.FLICKR_SECRET or "fake secret"
            },
            "collection_set": {
                "id": "test_collection_set"
            },
            "options": {}
        }

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_nsid(self, mock_photo_method):
        self.harvester.message["seeds"].append({"uid": "131866249@N02",
                                                "id":"1"})
        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        self.assertEqual(12, mock_photo_method.call_count)
        self.assertEqual(call(u'16610484049', u'ee80d9ecdc', ('Thumbnail', 'Large', 'Original')),
                         mock_photo_method.mock_calls[0])

        # Check harvest result
        self.assertTrue(self.harvester.harvest_result.success)
        self.assertEqual(1, len(self.harvester.harvest_result.token_updates))
        self.assertEqual("justin.littman", self.harvester.harvest_result.token_updates["1"])
        self.assertEqual(0, len(self.harvester.harvest_result.uids))

        # Check state store
        self.assertEqual("16609036938",
                         self.harvester.state_store.get_state("flickr_harvester", "131866249@N02.last_photo_id"))

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_username(self, mock_photo_method):
        self.harvester.message["seeds"].append({"token": "justin.littman",
                                                "id": "2"})
        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        self.assertEqual(12, mock_photo_method.call_count)
        self.assertEqual(call(u'16610484049', u'ee80d9ecdc', ('Thumbnail', 'Large', 'Original')),
                         mock_photo_method.mock_calls[0])

        # Check harvest result
        self.assertTrue(self.harvester.harvest_result.success)
        self.assertEqual(0, len(self.harvester.harvest_result.token_updates))
        self.assertEqual(1, len(self.harvester.harvest_result.uids))
        self.assertEqual("131866249@N02", self.harvester.harvest_result.uids["2"])

        # Check state store
        self.assertEqual("16609036938",
                         self.harvester.state_store.get_state("flickr_harvester", "131866249@N02.last_photo_id"))

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_update_username(self, mock_photo_method):
        self.harvester.message["seeds"].append({"uid": "131866249@N02",
                                                "token": "not_justin_littman",
                                                "id": "3"})
        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        self.assertEqual(12, mock_photo_method.call_count)
        self.assertEqual(call(u'16610484049', u'ee80d9ecdc', ('Thumbnail', 'Large', 'Original')),
                         mock_photo_method.mock_calls[0])

        # Check harvest result
        self.assertTrue(self.harvester.harvest_result.success)
        self.assertEqual(1, len(self.harvester.harvest_result.token_updates))
        self.assertEqual("justin.littman", self.harvester.harvest_result.token_updates["3"])
        self.assertEqual(0, len(self.harvester.harvest_result.uids))

        # Check state store
        self.assertEqual("16609036938",
                         self.harvester.state_store.get_state("flickr_harvester", "131866249@N02.last_photo_id"))

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_unknown_username(self, mock_photo_method):
        self.harvester.message["seeds"].append({"token": "not_justin_littman",
                                                "id": "4"})
        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        mock_photo_method.assert_not_called()

        # Check harvest result
        self.assertTrue(self.harvester.harvest_result.success)
        self.assertFalse(self.harvester.harvest_result.stats_summary()["flickr photos"])
        self.assertEqual(0, len(self.harvester.harvest_result.token_updates))
        self.assertEqual(0, len(self.harvester.harvest_result.uids))
        self.assertEqual(1, len(self.harvester.harvest_result.warnings))
        self.assertEqual(CODE_TOKEN_NOT_FOUND, self.harvester.harvest_result.warnings[0].code)

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_incremental_middle(self, mock_photo_method):
        self.harvester.message["seeds"].append({"uid": "131866249@N02",
                                                "id": "5"})
        self.harvester.message["options"]["incremental"] = True
        # Set state
        self.harvester.state_store.set_state("flickr_harvester", "131866249@N02.last_photo_id", "16609252490")

        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        self.assertEqual(5, mock_photo_method.call_count)
        self.assertEqual(call(u'16609252680', u'cf6e1840e9', ('Thumbnail', 'Large', 'Original')),
                         mock_photo_method.mock_calls[0])

        # Check harvest result
        self.assertTrue(self.harvester.harvest_result.success)
        self.assertEqual(1, len(self.harvester.harvest_result.token_updates))
        self.assertEqual("justin.littman", self.harvester.harvest_result.token_updates["5"])
        self.assertEqual(0, len(self.harvester.harvest_result.uids))

        # Check state store
        self.assertEqual("16609036938",
                         self.harvester.state_store.get_state("flickr_harvester", "131866249@N02.last_photo_id"))

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_incremental_end(self, mock_photo_method):
        self.harvester.message["seeds"].append({"uid": "131866249@N02",
                                                "id": "6"})
        self.harvester.message["options"]["incremental"] = True
        # Set state
        self.harvester.state_store.set_state("flickr_harvester", "131866249@N02.last_photo_id", "16609036938")

        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        mock_photo_method.assert_not_called()

        # Check harvest result
        self.assertTrue(self.harvester.harvest_result.success)
        self.assertEqual(1, len(self.harvester.harvest_result.token_updates))
        self.assertEqual("justin.littman", self.harvester.harvest_result.token_updates["6"])
        self.assertEqual(0, len(self.harvester.harvest_result.uids))

        # Check state store
        self.assertEqual("16609036938",
                         self.harvester.state_store.get_state("flickr_harvester", "131866249@N02.last_photo_id"))

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_unknown_nsid(self, mock_photo_method):
        self.harvester.message["seeds"].append({"uid": "x131866249@N02",
                                                "id": "7"})
        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        mock_photo_method.assert_not_called()

        # Check harvest result
        self.assertTrue(self.harvester.harvest_result.success)
        self.assertFalse(self.harvester.harvest_result.stats_summary()["flickr photos"])
        self.assertEqual(0, len(self.harvester.harvest_result.token_updates))
        self.assertEqual(0, len(self.harvester.harvest_result.uids))
        self.assertEqual(1, len(self.harvester.harvest_result.warnings))
        self.assertEqual(CODE_UID_NOT_FOUND, self.harvester.harvest_result.warnings[0].code)

    @vcr.use_cassette()
    def test_photo(self):
        self.harvester._create_api()
        self.harvester._photo("16609036938", "6ed7e2331e", ("Thumbnail", "Large", "not_Original"))

        # Check harvest result
        self.assertTrue(self.harvester.harvest_result.success)
        self.assertEqual(1, self.harvester.harvest_result.stats_summary()["flickr photos"])
        self.assertEqual(["https://farm9.staticflickr.com/8710/16609036938_6ed7e2331e_t.jpg",
                          "https://farm9.staticflickr.com/8710/16609036938_6ed7e2331e_b.jpg"],
                         self.harvester.harvest_result.urls)


@unittest.skipIf(not tests.test_config_available, "Skipping test since test config not available.")
@unittest.skipIf(not tests.integration_env_available, "Skipping test since integration env not available.")
class TestFlickrHarvesterIntegration(tests.TestCase):
    def _create_connection(self):
        return Connection(hostname="mq", userid=tests.mq_username, password=tests.mq_password)

    def setUp(self):
        self.exchange = Exchange(EXCHANGE, type="topic")
        self.result_queue = Queue(name="result_queue", routing_key="harvest.status.flickr.*", exchange=self.exchange,
                                  durable=True)
        self.web_harvest_queue = Queue(name="web_harvest_queue", routing_key="harvest.start.web", exchange=self.exchange)
        self.warc_created_queue = Queue(name="warc_created_queue", routing_key="warc_created", exchange=self.exchange)
        flickr_harvester_queue = Queue(name="flickr_harvester", exchange=self.exchange)
        with self._create_connection() as connection:
            self.result_queue(connection).declare()
            self.result_queue(connection).purge()
            self.web_harvest_queue(connection).declare()
            self.web_harvest_queue(connection).purge()
            self.warc_created_queue(connection).declare()
            self.warc_created_queue(connection).purge()
            # By declaring this, avoid race situation where harvester may not be up yet.
            flickr_harvester_queue(connection).declare()
            flickr_harvester_queue(connection).purge()

        self.harvest_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.harvest_path, ignore_errors=True)

    def test_user(self):
        harvest_msg = {
            "id": "test:1",
            "type": "flickr_user",
            "path": self.harvest_path,
            "seeds": [{
                "id": "seed_id",
                "uid": "131866249@N02"
            }],
            "credentials": {
                "key": tests.FLICKR_KEY,
                "secret": tests.FLICKR_SECRET
            },
            "collection_set": {
                "id": "test_collection_set"
            }
        }

        with self._create_connection() as connection:
            bound_exchange = self.exchange(connection)
            producer = Producer(connection, exchange=bound_exchange)
            producer.publish(harvest_msg, routing_key="harvest.start.flickr.flickr_user")

            # Now wait for result message.
            counter = 0
            bound_result_queue = self.result_queue(connection)
            message_obj = None
            while counter < 240 and not message_obj:
                time.sleep(.5)
                message_obj = bound_result_queue.get(no_ack=True)
                counter += 1
            self.assertTrue(message_obj, "Timed out waiting for result at {}.".format(datetime.now()))
            result_msg = message_obj.payload
            # Matching ids
            self.assertEqual("test:1", result_msg["id"])
            # Success
            self.assertEqual("completed success", result_msg["status"])
            # And some photos
            self.assertTrue(result_msg["stats"][date.today().isoformat()]["flickr photos"])

            # Web harvest message.
            bound_web_harvest_queue = self.web_harvest_queue(connection)
            message_obj = bound_web_harvest_queue.get(no_ack=True)
            self.assertIsNotNone(message_obj, "No web harvest message.")
            web_harvest_msg = message_obj.payload
            # Some seeds
            self.assertTrue(len(web_harvest_msg["seeds"]))

            # Warc created message.
            bound_warc_created_queue = self.warc_created_queue(connection)
            message_obj = bound_warc_created_queue.get(no_ack=True)
            self.assertIsNotNone(message_obj, "No warc created message.")
