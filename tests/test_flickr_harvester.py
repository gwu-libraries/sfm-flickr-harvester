from __future__ import absolute_import
import tests
import flickr_harvester
from harvester.state_store import DictHarvestStateStore
import json
import os
import tempfile
import hashlib
import datetime
import unittest
import pika
import shutil
import time


@unittest.skipIf(not tests.test_config_available, "Skipping test since test config not available.")
@unittest.skipIf(tests.integration_env_available, "Skipping test since integration env is available.")
class TestFlickrHarvester(tests.TestCase):
    def setUp(self):
        self.state_store = DictHarvestStateStore()
        self.harvester = flickr_harvester.FlickrHarvester(tests.FLICKR_KEY, tests.FLICKR_SECRET,
                                                          state_store=self.state_store)

    def test_bad_request(self):
        harv_resp, warc_records = self.harvester.harvest_user()
        self.assertFalse(harv_resp)
        self.assertEqual(harv_resp.errors[0]["code"], "FLICKR_BAD_REQUEST")
        self.assertFalse(harv_resp.warnings)

    def test_lookup_nsid(self):
        self.assertEqual("131866249@N02", self.harvester.lookup_nsid("justin.littman"))

    def test_lookup_no_nsid(self):
        self.assertIsNone(self.harvester.lookup_nsid("xjustin.littman"))

    def test_hostname(self):
        self.assertEqual("api.flickr.com", self.harvester.hostname)

    def test_user_and_photos(self):
        #Using a smaller per_page for testing
        harv_resp, warc_records = self.harvester.harvest_user(username="justin.littman", per_page=5, incremental=False,
                                                              sizes=("Thumbnail", "Original"))
        #Success
        self.assertTrue(harv_resp)

        #Warc records
        #1 people.getInfo x 2
        #3 pages people.getPublicPhotos x 2
        #12 photos.getInfo x 2
        self.assertEqual(32, len(warc_records))
        #First should be a request
        self.assertEqual(warc_records[0].type, "request")
        #And second a response
        self.assertEqual(warc_records[1].type, "response")

        #Urls
        #2 per photo
        self.assertEqual(24, len(harv_resp.urls))

        #Messages
        #Since querying by username, should get info message for nsid
        self.assertEqual(1, len(harv_resp.infos))
        self.assertEqual("FLICKR_NSID", harv_resp.infos[0]["code"])
        self.assertFalse(harv_resp.warnings)
        self.assertFalse(harv_resp.errors)

        #Summary
        self.assertEqual(1, harv_resp.summary["user"])
        self.assertEqual(12, harv_resp.summary["photo"])

    def test_incremental_user_no_prev_state(self):
        #No state set in state store

        #Using a smaller per_page for testing
        harv_resp, warc_records = self.harvester.harvest_user(username="justin.littman", per_page=5, incremental=True,
                                                              sizes=("Thumbnail", "Original"))
        #Success
        self.assertTrue(harv_resp)

        #Warc records
        #1 people.getInfo x 2
        #3 pages people.getPublicPhotos x 2
        #12 photos.getInfo x 2
        self.assertEqual(32, len(warc_records))

        #Urls
        #2 per photo
        self.assertEqual(24, len(harv_resp.urls))

    def test_incremental_user_no_new(self):
        #Set state in store
        self.state_store.set_state("flickr_harvester", "131866249@N02.last_photo_id", "16609036938")

        #Using a smaller per_page for testing
        harv_resp, warc_records = self.harvester.harvest_user(username="justin.littman", per_page=5, incremental=True,
                                                              sizes=("Thumbnail", "Original"))
        #Success
        self.assertTrue(harv_resp)

        #Warc records
        #1 people.getInfo x 2
        self.assertEqual(2, len(warc_records))

        #Urls
        self.assertEqual(0, len(harv_resp.urls))

        #Summary
        self.assertEqual(1, harv_resp.summary["user"])
        self.assertEqual(0, harv_resp.summary["photo"])

        #State not changed
        self.assertEqual("16609036938", self.state_store.get_state("flickr_harvester", "131866249@N02.last_photo_id"))

    def test_incremental_user(self):
        #16609252680 is the third on the second page
        #Set state in store
        self.state_store.set_state("flickr_harvester", "131866249@N02.last_photo_id", "16609252680")

        #Using a smaller per_page for testing
        harv_resp, warc_records = self.harvester.harvest_user(username="justin.littman", per_page=5, incremental=True,
                                                              sizes=("Thumbnail", "Original"))
        #Success
        self.assertTrue(harv_resp)

        #Warc records
        #1 people.getInfo x 2
        #2 pages people.getPublicPhotos x 2
        #4 photos.getInfo (2 from last page + 2 from second page) x 2
        self.assertEqual(14, len(warc_records))

        #Urls
        self.assertEqual(8, len(harv_resp.urls))

        #Summary
        self.assertEqual(1, harv_resp.summary["user"])
        self.assertEqual(4, harv_resp.summary["photo"])

        #State changed
        self.assertEqual("16609036938", self.state_store.get_state("flickr_harvester", "131866249@N02.last_photo_id"))

    def test_incremental_user_first_on_page(self):
        #16176690903 is the first on the second page
        #Set state in store
        self.state_store.set_state("flickr_harvester", "131866249@N02.last_photo_id", "16176690903")

        #Using a smaller per_page for testing
        harv_resp, warc_records = self.harvester.harvest_user(username="justin.littman", per_page=5, incremental=True,
                                                              sizes=("Thumbnail", "Original"))
        #Success
        self.assertTrue(harv_resp)

        #Warc records
        #1 people.getInfo x 2
        #2 pages people.getPublicPhotos x 2
        #6 photos.getInfo (4 from second page + 2 from last page) x 2
        self.assertEqual(18, len(warc_records))

        #Urls
        self.assertEqual(12, len(harv_resp.urls))

        #State changed
        self.assertEqual("16609036938", self.state_store.get_state("flickr_harvester", "131866249@N02.last_photo_id"))

    def test_incremental_user_last_on_page(self):
        #16610484809 is the last on the second page
        #Set state in store
        self.state_store.set_state("flickr_harvester", "131866249@N02.last_photo_id", "16610484809")

        #Using a smaller per_page for testing
        harv_resp, warc_records = self.harvester.harvest_user(username="justin.littman", per_page=5, incremental=True,
                                                              sizes=("Thumbnail", "Original"))
        #Success
        self.assertTrue(harv_resp)

        #Warc records
        #1 people.getInfo x 2
        #1 pages people.getPublicPhotos x 2
        #2 photos.getInfo (2 from last page) x 2
        self.assertEqual(8, len(warc_records))

        #Urls
        self.assertEqual(4, len(harv_resp.urls))

        #State changed
        self.assertEqual("16609036938", self.state_store.get_state("flickr_harvester", "131866249@N02.last_photo_id"))


@unittest.skipIf(not tests.test_config_available, "Skipping test since test config not available.")
@unittest.skipIf(tests.integration_env_available, "Skipping test since integration env is available.")
class TestFlickrConsumer(tests.TestCase):
    def setUp(self):
        self.state_store = DictHarvestStateStore()
        self.consumer = flickr_harvester.FlickrConsumer(None, state_store=self.state_store)
        self.fake_channel = FakeChannel()
        self.fake_method = FakeMethod()

    def test_user_and_photos_by_message(self):
        message = {
            "id": "test:1",
            "type": "flickr_user",
            "seeds": [
                {
                    "nsid": "131866249@N02",
                    "sizes": ["Thumbnail", "Original"]
                }
            ],
            "credentials": {
                "key": tests.FLICKR_KEY,
                "secret": tests.FLICKR_SECRET
            },
            "collection": {
                "id": "test_collection",
                "path": "%s/test_collection" % tempfile.mkdtemp()
            }
        }

        self.consumer._callback(self.fake_channel, self.fake_method, None, json.dumps(message))
        self.assertEqual(3, len(self.fake_channel.messages))

        #A warc_created message
        exchange, routing_key, properties, body = self.fake_channel.messages[0]
        #Check exchange
        self.assertEqual("sfm_exchange", exchange)
        #Check routing key
        self.assertEqual("warc_created", routing_key)
        #Check properties
        self.assertEqual("application/json", properties.content_type)
        self.assertEqual(2, properties.delivery_mode)
        #Check message
        warc_path = body["warc"]["path"]
        self.assertTrue(os.path.exists(warc_path))
        self.assertEqual(os.path.getsize(warc_path), body["warc"]["bytes"])
        self.assertEqual(hashlib.sha1(open(warc_path).read()).hexdigest(), body["warc"]["sha1"])
        self.assertEqual("test_collection", body["collection"]["id"])
        #With 14 api called
        self.assertEqual(14, len(body["apis_called"]))
        #First is people.getInfo
        self.assertEqual("flickr", body["apis_called"][0]["api"]["platform"])
        self.assertEqual("people.getInfo", body["apis_called"][0]["api"]["method"])
        self.assertEqual(["131866249@N02"], body["apis_called"][0]["api"]["parameters"]["user_id"])
        self.assertTrue(body["apis_called"][0]["url"].startswith("https://api.flickr.com/services/rest"))
        self.assertTrue(datetime.datetime.strptime(body["apis_called"][0]["date_harvested"], "%Y-%m-%dT%H:%M:%S"))
        self.assertTrue(body["apis_called"][0]["response_record"]["id"])
        #And then a bunch of photos.getInfo
        self.assertEqual("photos.getInfo", body["apis_called"][1]["api"]["method"])
        #And a people.getPublicPhotos
        self.assertEqual("people.getPublicPhotos", body["apis_called"][13]["api"]["method"])

        #And a harvest.web message
        exchange, routing_key, properties, body = self.fake_channel.messages[1]
        self.assertEqual("harvest.start.web", routing_key)
        self.assertEqual("web", body["type"])
        self.assertEqual(24, len(body["seeds"]))
        self.assertTrue(body["seeds"][0]["token"])
        self.assertIsNotNone(body["id"])
        self.assertEqual("test:1", body["parent_id"])

        #And a status message
        exchange, routing_key, properties, body = self.fake_channel.messages[2]
        self.assertEqual("harvest.status.flickr.flickr_user", routing_key)
        self.assertEqual("test:1", body["id"])
        self.assertEqual("completed success", body["status"])
        self.assertEqual([], body["infos"])
        self.assertEqual([], body["warnings"])
        self.assertEqual([], body["errors"])
        self.assertTrue(body["date_ended"])
        self.assertTrue(body["date_started"])
        self.assertEqual(1, body["summary"]["user"])
        self.assertEqual(12, body["summary"]["photo"])

    def test_nothing_harvested_by_message(self):
        message = {
            "id": "test:2",
            "type": "flickr_user",
            "seeds": [
            ],
            "credentials": {
                "key": tests.FLICKR_KEY,
                "secret": tests.FLICKR_SECRET
            },
            "collection": {
                "id": "test_collection",
                "path": "%s/test_collection" % tempfile.mkdtemp()
            }
        }

        self.consumer._callback(self.fake_channel, self.fake_method, None, json.dumps(message))
        #Only a result message
        self.assertEqual(1, len(self.fake_channel.messages))
        exchange, routing_key, properties, body = self.fake_channel.messages[0]
        self.assertEqual("harvest.status.flickr.flickr_user", routing_key)
        self.assertIsNone(body["summary"].get("user"))
        self.assertIsNone(body["summary"].get("photo"))

    def test_not_found_user_by_message(self):
        message = {
            "id": "test:5",
            "type": "flickr_user",
            "seeds": [
                {
                    "nsid": "x131866249@N02",
                    "sizes": ["Thumbnail", "Original"]
                }
            ],
            "credentials": {
                "key": tests.FLICKR_KEY,
                "secret": tests.FLICKR_SECRET
            },
            "collection": {
                "id": "test_collection",
                "path": "%s/test_collection" % tempfile.mkdtemp()
            }
        }

        self.consumer._callback(self.fake_channel, self.fake_method, None, json.dumps(message))
        self.assertEqual(1, len(self.fake_channel.messages))

        #No warc_created message

        #And a status message with an error
        exchange, routing_key, properties, body = self.fake_channel.messages[0]
        self.assertEqual("harvest.status.flickr.flickr_user", routing_key)
        self.assertEqual("test:5", body["id"])
        self.assertEqual("completed failure", body["status"])
        self.assertEqual(1, len(body["errors"]))
        self.assertEqual("FLICKR_NOT_FOUND", body["errors"][0]["code"])

    def test_not_found_username_by_message(self):
        message = {
            "id": "test6",
            "type": "flickr_user",
            "seeds": [
                {
                    "username": "not_a_user",
                    "sizes": ["Thumbnail", "Original"]
                }
            ],
            "credentials": {
                "key": tests.FLICKR_KEY,
                "secret": tests.FLICKR_SECRET
            },
            "collection": {
                "id": "test_collection",
                "path": "%s/test_collection" % tempfile.mkdtemp()
            }
        }

        self.consumer._callback(self.fake_channel, self.fake_method, None, json.dumps(message))
        self.assertEqual(1, len(self.fake_channel.messages))

        #A status message with an error
        exchange, routing_key, properties, body = self.fake_channel.messages[0]
        self.assertEqual("harvest.status.flickr.flickr_user", routing_key)
        self.assertEqual("completed failure", body["status"])
        self.assertEqual(1, len(body["errors"]))
        self.assertEqual("FLICKR_NOT_FOUND", body["errors"][0]["code"])

@unittest.skipIf(not tests.test_config_available, "Skipping test since test config not available.")
@unittest.skipIf(not tests.integration_env_available, "Skipping test since integration env not available.")
class TestFlickrHarvesterIntegration(tests.TestCase):
    def setUp(self):
        credentials = pika.PlainCredentials(tests.mq_username, tests.mq_password)
        parameters = pika.ConnectionParameters(host="mq", credentials=credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        #Declare result queue
        result = self.channel.queue_declare(exclusive=True)
        self.result_queue = result.method.queue
        #Bind
        self.channel.queue_bind(exchange=flickr_harvester.EXCHANGE,
                                queue=self.result_queue, routing_key="harvest.status.flickr.*")
        #Declare web harvest queue
        result = self.channel.queue_declare(exclusive=True)
        self.web_harvest_queue = result.method.queue
        #Bind
        self.channel.queue_bind(exchange=flickr_harvester.EXCHANGE,
                                queue=self.web_harvest_queue, routing_key="harvest.start.web")
        #Declare warc_created queue
        result = self.channel.queue_declare(exclusive=True)
        self.warc_created_queue = result.method.queue
        #Bind
        self.channel.queue_bind(exchange=flickr_harvester.EXCHANGE,
                                queue=self.warc_created_queue, routing_key="warc_created")

        self.collection_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.collection_path, ignore_errors=True)
        self.channel.close()
        self.connection.close()

    def test_harvest(self):
        harvest_msg = {
            "id": "test:1",
            "type": "flickr_user",
            "seeds": [
                {
                    "nsid": "131866249@N02",
                    "sizes": ["Thumbnail", "Original"]
                }
            ],
            "credentials": {
                "key": tests.FLICKR_KEY,
                "secret": tests.FLICKR_SECRET
            },
            "collection": {
                "id": "test_collection",
                "path": self.collection_path

            }
        }
        self.channel.basic_publish(exchange=flickr_harvester.EXCHANGE,
                                   routing_key="harvest.start.flickr.flickr_user",
                                   properties=pika.spec.BasicProperties(content_type="application/json",
                                                                        delivery_mode=2),
                                   body=json.dumps(harvest_msg, indent=4))

        #Now wait for result message.
        result_body = None
        counter = 0
        while counter < 60:
            time.sleep(.5)
            method_frame, header_frame, result_body = self.channel.basic_get(self.result_queue)
            if result_body:
                self.channel.basic_ack(method_frame.delivery_tag)
                break
            counter += 1
        self.assertTrue(result_body, "Timed out waiting for result.")
        result_msg = json.loads(result_body)
        #Matching ids
        self.assertEqual("test:1", result_msg["id"])
        #Success
        self.assertEqual("completed success", result_msg["status"])
        #1 photo
        self.assertEqual(1, result_msg["summary"]["user"])

        #Web harvest message.
        method_frame, header_frame, web_harvest_body = self.channel.basic_get(self.web_harvest_queue)
        self.assertTrue(web_harvest_body, "No web harvest message.")
        web_harvest_msg = json.loads(web_harvest_body)
        self.assertEqual(24, len(web_harvest_msg["seeds"]))

        #Warc created message.
        method_frame, header_frame, warc_created_body = self.channel.basic_get(self.warc_created_queue)
        self.assertTrue(web_harvest_body, "No warc created message.")
        warc_created_msg = json.loads(warc_created_body)
        self.assertEqual(14, len(warc_created_msg["apis_called"]))
        self.assertEqual("people.getInfo", warc_created_msg["apis_called"][0]["api"]["method"])


class FakeChannel():
    def __init__(self):
        self.messages = []

    def basic_publish(self, exchange=None, routing_key=None, properties=None, body=None):
        self.messages.append((exchange, routing_key, properties, json.loads(body)))

    def basic_ack(self, delivery_tag=None):
        pass


class FakeMethod():
    def __init__(self):
        self.delivery_tag = 1
