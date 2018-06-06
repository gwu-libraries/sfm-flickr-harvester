import tests
import vcr as base_vcr
from flickr_harvester import FlickrHarvester
from sfmutils.state_store import DictHarvestStateStore
from sfmutils.harvester import HarvestResult, CODE_TOKEN_NOT_FOUND, CODE_UID_NOT_FOUND, EXCHANGE, STATUS_RUNNING, \
    STATUS_SUCCESS
from sfmutils.warc_iter import IterItem
from flickr_warc_iter import FlickrWarcIter, TYPE_FLICKR_PHOTO, TYPE_FLICKR_SIZES
from mock import patch, call, MagicMock
import unittest
import time
from kombu import Connection, Exchange, Queue, Producer
import shutil
import tempfile
from datetime import datetime, date
import copy
import os
from tests.photo import photo1, size1

vcr = base_vcr.VCR(
    cassette_library_dir='tests/fixtures',
    record_mode='once',
)

base_message = {
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
    "collection": {
        "id": "test_collection"
    },
    "options": {}
}


class TestFlickrHarvester(tests.TestCase):
    def setUp(self):
        self.working_path = tempfile.mkdtemp()
        self.harvester = FlickrHarvester(self.working_path, per_page=6)
        self.harvester.state_store = DictHarvestStateStore()
        self.harvester.result = HarvestResult()

    def tearDown(self):
        if os.path.exists(self.working_path):
            shutil.rmtree(self.working_path)

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_nsid(self, mock_photo_method):
        message = copy.deepcopy(base_message)
        message["seeds"].append({"uid": "131866249@N02", "id": "1"})
        self.harvester.message = message
        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        self.assertEqual(12, mock_photo_method.call_count)
        self.assertEqual(call(u'16610484049', u'ee80d9ecdc'), mock_photo_method.mock_calls[0])

        # Check harvest result
        self.assertEqual("justin.littman", self.harvester.result.token_updates["1"])
        self.assertEqual(0, len(self.harvester.result.uids))

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_username(self, mock_photo_method):
        message = copy.deepcopy(base_message)
        message["seeds"].append({"token": "justin.littman", "id": "2"})
        self.harvester.message = message

        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        self.assertEqual(12, mock_photo_method.call_count)
        self.assertEqual(call(u'16610484049', u'ee80d9ecdc'), mock_photo_method.mock_calls[0])

        # Check harvest result
        self.assertEqual(1, len(self.harvester.result.uids))
        self.assertEqual("131866249@N02", self.harvester.result.uids["2"])

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_update_username(self, mock_photo_method):
        message = copy.deepcopy(base_message)
        message["seeds"].append({"uid": "131866249@N02",
                                 "token": "not_justin_littman",
                                 "id": "3"})
        self.harvester.message = message

        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        self.assertEqual(12, mock_photo_method.call_count)
        self.assertEqual(call(u'16610484049', u'ee80d9ecdc'), mock_photo_method.mock_calls[0])

        # Check harvest result
        self.assertTrue(self.harvester.result.success)
        self.assertEqual(1, len(self.harvester.result.token_updates))
        self.assertEqual("justin.littman", self.harvester.result.token_updates["3"])
        self.assertEqual(0, len(self.harvester.result.uids))

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_unknown_username(self, mock_photo_method):
        message = copy.deepcopy(base_message)
        message["seeds"].append({"token": "not_justin_littman",
                                 "id": "4"})
        self.harvester.message = message

        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        mock_photo_method.assert_not_called()

        # Check harvest result
        self.assertTrue(self.harvester.result.success)
        self.assertEqual(0, len(self.harvester.result.token_updates))
        self.assertEqual(0, len(self.harvester.result.uids))
        self.assertEqual(1, len(self.harvester.result.warnings))
        self.assertEqual(CODE_TOKEN_NOT_FOUND, self.harvester.result.warnings[0].code)

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_incremental_middle(self, mock_photo_method):
        message = copy.deepcopy(base_message)
        message["seeds"].append({"uid": "131866249@N02",
                                 "id": "5"})
        message["options"]["incremental"] = True
        self.harvester.message = message

        # Set state
        self.harvester.state_store.set_state("flickr_harvester", "131866249@N02.last_photo_id", "16609252490")

        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        self.assertEqual(6, mock_photo_method.call_count)
        self.assertEqual(call(u'16610484049', u'ee80d9ecdc'), mock_photo_method.mock_calls[0])

        # Check harvest result
        self.assertTrue(self.harvester.result.success)
        self.assertEqual(1, len(self.harvester.result.token_updates))
        self.assertEqual("justin.littman", self.harvester.result.token_updates["5"])
        self.assertEqual(0, len(self.harvester.result.uids))

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_incremental_end(self, mock_photo_method):
        message = copy.deepcopy(base_message)
        message["seeds"].append({"uid": "131866249@N02",
                                 "id": "6"})
        message["options"]["incremental"] = True
        self.harvester.message = message

        # Set state
        self.harvester.state_store.set_state("flickr_harvester", "131866249@N02.last_photo_id", "16610484049")

        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        mock_photo_method.assert_not_called()

        # Check harvest result
        self.assertTrue(self.harvester.result.success)
        self.assertEqual(1, len(self.harvester.result.token_updates))
        self.assertEqual("justin.littman", self.harvester.result.token_updates["6"])
        self.assertEqual(0, len(self.harvester.result.uids))

    @vcr.use_cassette()
    @patch.object(FlickrHarvester, "_photo")
    def test_harvest_unknown_nsid(self, mock_photo_method):
        message = copy.deepcopy(base_message)
        message["seeds"].append({"uid": "x131866249@N02",
                                 "id": "7"})
        self.harvester.message = message

        self.harvester.harvest_seeds()

        # Calls to _photo have been mocked out. Check mock.
        mock_photo_method.assert_not_called()

        # Check harvest result
        self.assertTrue(self.harvester.result.success)
        self.assertFalse(self.harvester.result.stats_summary()["flickr photos"])
        self.assertEqual(0, len(self.harvester.result.token_updates))
        self.assertEqual(0, len(self.harvester.result.uids))
        self.assertEqual(1, len(self.harvester.result.warnings))
        self.assertEqual(CODE_UID_NOT_FOUND, self.harvester.result.warnings[0].code)

    @vcr.use_cassette()
    def test_photo(self):
        self.harvester.message = base_message
        self.harvester._create_api()
        self.harvester._photo("16609036938", "6ed7e2331e")

        # Check harvest result
        self.assertTrue(self.harvester.result.success)
        self.assertEqual(1, self.harvester.result.harvest_counter["flickr photos"])

    @patch("flickr_harvester.FlickrWarcIter", autospec=True)
    def test_process(self, iter_class):
        message = copy.deepcopy(base_message)
        message["options"]["image_sizes"] = ["Thumbnail", "Original"]
        self.harvester.message = message

        mock_iter = MagicMock(spec=FlickrWarcIter)
        mock_iter.__iter__.side_effect = [[IterItem(TYPE_FLICKR_PHOTO, None, None, None, photo1),
                                           IterItem(TYPE_FLICKR_SIZES, None, None, None, size1)].__iter__()]
        # Return mock_iter when instantiating a TwitterRestWarcIter.
        iter_class.side_effect = [mock_iter]

        self.harvester.process_warc("test.warc.gz")

        self.assertEqual(1, self.harvester.result.stats_summary()["flickr photos"])

        # Check state store
        self.assertEqual("16609036938",
                         self.harvester.state_store.get_state("flickr_harvester", "131866249@N02.last_photo_id"))


@unittest.skipIf(not tests.test_config_available, "Skipping test since test config not available.")
@unittest.skipIf(not tests.integration_env_available, "Skipping test since integration env not available.")
class TestFlickrHarvesterIntegration(tests.TestCase):
    @staticmethod
    def _create_connection():
        return Connection(hostname="mq", userid=tests.mq_username, password=tests.mq_password)

    def setUp(self):
        self.exchange = Exchange(EXCHANGE, type="topic")
        self.result_queue = Queue(name="result_queue", routing_key="harvest.status.flickr.*", exchange=self.exchange,
                                  durable=True)
        self.web_harvest_queue = Queue(name="web_harvest_queue", routing_key="harvest.start.web",
                                       exchange=self.exchange)
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

        self.harvest_path = None

    def tearDown(self):
        if self.harvest_path:
            shutil.rmtree(self.harvest_path, ignore_errors=True)

    def test_user(self):
        self.harvest_path = "/sfm-data/collection_set/test_collection/test_1"
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
            },
            "collection": {
                "id": "test_collection"
            }
        }

        with self._create_connection() as connection:
            bound_exchange = self.exchange(connection)
            producer = Producer(connection, exchange=bound_exchange)
            producer.publish(harvest_msg, routing_key="harvest.start.flickr.flickr_user")

            # Now wait for status message.
            status_msg = self._wait_for_message(self.result_queue, connection)
            # Matching ids
            self.assertEqual("test:1", status_msg["id"])
            # Running
            self.assertEqual(STATUS_RUNNING, status_msg["status"])

            # Another running message
            status_msg = self._wait_for_message(self.result_queue, connection)
            self.assertEqual(STATUS_RUNNING, status_msg["status"])

            # Now wait for result message.
            result_msg = self._wait_for_message(self.result_queue, connection)
            # Matching ids
            self.assertEqual("test:1", result_msg["id"])
            # Success
            self.assertEqual(STATUS_SUCCESS, result_msg["status"])
            # And some photos
            self.assertTrue(result_msg["stats"][date.today().isoformat()]["flickr photos"])

            # Web harvest message.
            web_harvest_msg = self._wait_for_message(self.web_harvest_queue, connection)
            # Some seeds
            self.assertTrue(len(web_harvest_msg["seeds"]))

            # Warc created message.
            warc_msg = self._wait_for_message(self.warc_created_queue, connection)
            # check path exist
            self.assertTrue(os.path.isfile(warc_msg["warc"]["path"]))

    def _wait_for_message(self, queue, connection):
        counter = 0
        message_obj = None
        bound_result_queue = queue(connection)
        while counter < 180 and not message_obj:
            time.sleep(.5)
            message_obj = bound_result_queue.get(no_ack=True)
            counter += 1
        self.assertIsNotNone(message_obj, "Timed out waiting for result at {}.".format(datetime.now()))
        return message_obj.payload
