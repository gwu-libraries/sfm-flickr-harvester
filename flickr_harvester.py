import logging
import flickrapi
import httplib as http_client
from harvester.httplib_helper import wrap_api_call
import json
import urlparse
from harvester.response import HarvestResponse
from harvester.state_store import NullHarvestStateStore
import argparse
import pika
from harvester.warc import WarcWriterOpen, generate_warc_info
import os
import hashlib
import datetime
import sys

log = logging.getLogger(__name__)

FLICKR_HOST = "https://api.flickr.com"
QUEUE = "flickr_harvester"
EXCHANGE = "sfm_exchange"
ROUTING_KEY = "harvest.start.flickr.*"


class FlickrHarvester():
    def __init__(self, key, secret, state_store=None):
        self.api = flickrapi.FlickrAPI(key, secret, store_token=False)
        self.hostname = urlparse.urlparse(FLICKR_HOST).hostname
        self.state_store = state_store or NullHarvestStateStore()

    def harvest_user(self, username=None, nsid=None, incremental=True, per_page=None, sizes=None):
        log.info("Harvesting user %s. Incremental=%s.", username or nsid, incremental)
        #Per_page is intended for testing only.
        harv_resp = HarvestResponse()
        warc_records = []
        if not (username or nsid):
            harv_resp.errors.append({"code": "FLICKR_BAD_REQUEST", "message": "Username or nsid not provided."})
            harv_resp.success = False
        if harv_resp:
            #Lookup nsid if don't already know
            if not nsid:
                log.debug("Looking up nsid for %s", username)
                nsid = self.lookup_nsid(username)
                if nsid:
                    harv_resp.infos.append({"code": "FLICKR_NSID", "message": "NSID for %s is %s." % (username, nsid)})
                else:
                    harv_resp.errors.append({"code": "FLICKR_NOT_FOUND",
                                             "message": "Username %s is not found." % username})
                    harv_resp.success = False

        if harv_resp:
            #Get info on the user
            #Setting format=json will return raw json.
            request_record, response_record, raw_json_resp = wrap_api_call(
                lambda: self.api.people.getInfo(user_id=nsid, format='json'),
                http_client.HTTPConnection,
                FLICKR_HOST
            )

            json_resp = json.loads(raw_json_resp)
            if json_resp["stat"] != "ok":
                if json_resp["code"] == 1:
                    log.warning("User %s not found" % nsid)
                    harv_resp.errors.append({"code": "FLICKR_NOT_FOUND",
                                             "message": "User with nsid %s not found." % nsid})
                else:
                    harv_resp.errors.append({"code": "FLICKR_ERROR", "message": json_resp["message"]})
                harv_resp.success = False

            if harv_resp:
                #Write request
                warc_records.append(request_record)
                #Write response
                warc_records.append(response_record)
                #Update summary
                harv_resp.increment_summary("user")

                #Get first page of public photos to get number of pages and photos
                json_resp = self.api.people.getPublicPhotos(user_id=nsid, format='parsed-json', per_page=per_page)
                total_pages = json_resp["photos"]["pages"]

                last_photo_id = self.state_store.get_state(__name__, "%s.last_photo_id" % nsid) \
                    if incremental else False
                new_last_photo_id = None
                found_last_photo_id = False
                #Going through pages backward
                for page in reversed(range(1, total_pages+1)):
                    log.debug("Fetching %s of %s pages.", page, total_pages)
                    request_record, response_record, raw_json_resp = wrap_api_call(
                        lambda: self.api.people.getPublicPhotos(user_id=nsid, format='json',
                                                                per_page=per_page, page=page),
                        http_client.HTTPConnection,
                        FLICKR_HOST
                    )
                    json_resp = json.loads(raw_json_resp)
                    #Going through photos backwards
                    harvested_photo = False
                    for photo in reversed(json_resp["photos"]["photo"]):
                        photo_id = photo["id"]
                        if new_last_photo_id is None and photo_id != last_photo_id:
                            new_last_photo_id = photo_id
                        log.debug("Photo %s" % photo_id)
                        if photo_id == last_photo_id:
                            log.debug("Is last photo")
                            found_last_photo_id = True
                            break
                        harvested_photo = True
                        photo_harv_resp, photo_warc_records = self.harvest_photo(photo["id"], photo["secret"],
                                                                                 sizes=sizes)
                        if photo_harv_resp:
                            warc_records.extend(photo_warc_records)
                        harv_resp.merge(photo_harv_resp)

                    if harvested_photo:
                        log.debug("Added photos for this page, so writing request and response")
                        #Append request and response
                        warc_records.append(request_record)
                        warc_records.append(response_record)
                    if found_last_photo_id:
                        break

                if new_last_photo_id:
                    log.debug("New last photo id is %s", new_last_photo_id)
                    self.state_store.set_state(__name__, "%s.last_photo_id" % nsid, new_last_photo_id)

        return harv_resp, warc_records

    def harvest_photo(self, photo_id, secret=None, sizes=None):
        log.info("Harvesting photo %s.", photo_id)

        if not sizes:
            sizes = ("Thumbnail", "Large", "Original")

        harv_resp = HarvestResponse()
        warc_records = []

        #Get info on the photo
        request_record, response_record, raw_json_resp = wrap_api_call(
            lambda: self.api.photos.getInfo(photo_id=photo_id, secret=secret, format='json'),
            http_client.HTTPConnection,
            FLICKR_HOST
        )

        json_resp = json.loads(raw_json_resp)
        if json_resp["stat"] != "ok":
            if json_resp["code"] == 1:
                log.warning("Photo %s not found" % photo_id)
                harv_resp.errors.append({"code": "FLICKR_NOT_FOUND", "message": "Photo %s not found." % photo_id})
            else:
                harv_resp.errors.append({"code": "FLICKR_ERROR", "message": json_resp["message"]})
            harv_resp.success = False

        if harv_resp:
            #Append request and response
            warc_records.append(request_record)
            warc_records.append(response_record)
            #Update summary
            harv_resp.increment_summary("photo")

            #Call getSizes, but don't record
            sizes_json_resp = self.api.photos.getSizes(photo_id=photo_id, format='parsed-json')
            for size in sizes_json_resp["sizes"]["size"]:
                if size["label"] in sizes:
                    log.debug("Adding url for %s", size["label"])
                    harv_resp.urls.append(size["source"])
                else:
                    log.debug("Skipping url for %s", size["label"])

        return harv_resp, warc_records

    def lookup_nsid(self, username):
        """
        Lookup a user's nsid.
        :param username: Username to lookup.
        :return: The nsid or None if not found.
        """
        find_resp = self.api.people.findByUsername(username=username, format="parsed-json")
        nsid = None
        if find_resp["stat"] == "ok":
            nsid = find_resp["user"]["nsid"]
        log.debug("Looking up username %s returned %s", username, nsid)
        return nsid


class FlickrConsumer():
    def __init__(self, host=None, username=None, password=None, state_store=None):
        #Creating a connection can be skipped for testing purposes
        if host:
            assert username
            assert password
            credentials = pika.PlainCredentials(username, password)
            parameters = pika.ConnectionParameters(host=host, credentials=credentials)
            self._connection = pika.BlockingConnection(parameters)
            channel = self._connection.channel()
            #Declare sfm_exchange
            channel.exchange_declare(exchange=EXCHANGE,
                                     type="topic", durable=True)
            #Declare harvester queue
            channel.queue_declare(queue=QUEUE,
                                  durable=True)
            #Bind
            channel.queue_bind(exchange=EXCHANGE,
                               queue=QUEUE, routing_key=ROUTING_KEY)

            channel.close()

        self._state_store = state_store

    def consume(self):
        channel = self._connection.channel()
        channel.basic_qos(prefetch_count=1)
        log.info("Waiting for messages from %s" % QUEUE)
        channel.basic_consume(self._callback, queue=QUEUE)
        channel.start_consuming()

    def harvest_seeds(self, seed_str):
        """
        Process seeds as specified in seeds.
        """

        return self._callback(None, None, None, seed_str)

    def _callback(self, channel, method, properties, body):
        """
        Callback for receiving harvest message.

        Note that this handles channel, method, and properties being None,
        allowing it to be invoked from a non-mq context.
        """

        log.info("Harvesting by message")
        if channel:
            #Acknowledge the message
            log.debug("Acking message")
            channel.basic_ack(delivery_tag=method.delivery_tag)

        start_date = datetime.datetime.now()
        message = json.loads(body)
        log.debug("Message is %s" % json.dumps(message, indent=4))

        collection_id = message["collection"]["id"]
        collection_path = message["collection"]["path"]

        warc_id, warc_path, created_date = generate_warc_info(collection_path, collection_id, "flickr")

        #Setup FlickrHarvester
        key = message["credentials"]["key"]
        secret = message["credentials"]["secret"]
        harvester = FlickrHarvester(key, secret, state_store=self._state_store)

        with WarcWriterOpen(warc_path) as warc_writer:
            merged_harv_resp = HarvestResponse()
            #Collecting api called message parts to add to warc created message
            merged_api_called_message_parts = []
            harvest_type = message.get("type")
            log.debug("Seed type is %s", harvest_type)
            if harvest_type == "flickr_user":
                for seed in message.get("seeds"):
                    username = seed.get("username")
                    nsid = seed.get("nsid")
                    incremental = seed.get("incremental", True)
                    sizes = seed.get("sizes")
                    harv_resp, warc_records = harvester.harvest_user(username=username,
                                                                     nsid=nsid, incremental=incremental, sizes=sizes)
                    #If success, write to warc
                    if harv_resp:
                        warc_writer.write_records(warc_records)
                        merged_api_called_message_parts.extend(self._create_api_called_message_parts(warc_records))
                    #Merge harv_resp
                    merged_harv_resp.merge(harv_resp)
            else:
                merged_harv_resp.errors.append({"code": "FLICKR_UNKNOWN_SEED_TYPE",
                                                "message": "%s is an unknown seed type for flickr" % harvest_type})
                merged_harv_resp.success = False

        #If nothing was written to warc, it will delete itself. Only send message if it exists.
        if os.path.exists(warc_path):
            self._send_warc_created_message(channel, collection_id, collection_path, warc_id, warc_path,
                                            created_date, merged_api_called_message_parts)
        else:
            log.debug("Skipping sending warc created message for %s", warc_id)

        harvest_id = message["id"]
        #Send a new harvest message for urls
        if merged_harv_resp.urls:
            self._send_web_harvest_message(channel, harvest_id, collection_id, collection_path,
                                               merged_harv_resp.urls_as_set())
        else:
            log.debug("No url seeds for %s", warc_id)

        #Send result message
        self._send_status_message(channel, harvest_id, merged_harv_resp, harvest_type, start_date)

        return merged_harv_resp

    @staticmethod
    def _send_warc_created_message(channel, collection_id, collection_path, warc_id, warc_path, created_date,
                                   api_called_message_parts):
        assert os.path.exists(warc_path)
        message = {
            "collection": {
                "id": collection_id,
                "path": collection_path

            },
            "warc": {
                "id": warc_id,
                "path": warc_path,
                "date_created": created_date.isoformat(),
                "bytes": os.path.getsize(warc_path),
                "sha1": hashlib.sha1(open(warc_path).read()).hexdigest()
            },
            "apis_called": api_called_message_parts
        }
        FlickrConsumer._publish_message(channel, "warc_created", message)

    @staticmethod
    def _create_api_called_message_parts(warc_records):
        message_parts = []
        for request_record, response_record in zip(*[iter(warc_records)]*2):
            api_method, api_parameters = parse_flickr_api_url(request_record.url)
            message_parts.append({
                "date_harvested": datetime.datetime.strptime(response_record.date, '%Y-%m-%dT%H:%M:%SZ').isoformat(),
                "url": request_record.url,
                "api": {
                    "platform": "flickr",
                    "method": api_method,
                    "parameters": api_parameters,
                },
                "request_record": {
                    "id": request_record["WARC-Record-ID"]
                },
                "response_record": {
                    "id": response_record["WARC-Record-ID"]
                }
            })
        return message_parts

    @staticmethod
    def _send_web_harvest_message(channel, harvest_id, collection_id, collection_path, urls):
        message = {
            #This will be unique
            "id": "flickr_harvester:%s" % harvest_id,
            "parent_id": harvest_id,
            "type": "web",
            "seeds": [],
            "collection": {
                "id": collection_id,
                "path": collection_path
            }
        }
        for url in urls:
            message["seeds"].append({"token": url})
        FlickrConsumer._publish_message(channel, "harvest.start.web", message)

    @staticmethod
    def _send_status_message(channel, harvest_id, harv_resp, type, start_date):
        #Just add additional info to job message
        message = {
            "id": harvest_id,
            "status": "completed success" if harv_resp.success else "completed failure",
            "infos": harv_resp.infos,
            "warnings": harv_resp.warnings,
            "errors": harv_resp.errors,
            "date_started": start_date.isoformat(),
            "date_ended": datetime.datetime.now().isoformat(),
            "summary": harv_resp.summary
        }
        #Routing key may be none
        status_routing_key = "harvest.status.flickr.%s" % type
        FlickrConsumer._publish_message(channel, status_routing_key, message)

    @staticmethod
    def _publish_message(channel, routing_key, message):
        message_body = json.dumps(message, indent=4)
        if channel:
            log.debug("Sending message to sfm_exchange with routing_key %s. The body is: %s", routing_key, message_body)
            channel.basic_publish(exchange=EXCHANGE,
                                  routing_key=routing_key,
                                  properties=pika.spec.BasicProperties(content_type="application/json",
                                                                       delivery_mode=2),
                                  body=message_body)
        else:
            log.debug("Not sending message with routing_key %s. The body is: %s", routing_key, message_body)


def parse_flickr_api_url(url):
    """
    Parse api method and arguments from url
    """
    #https://api.flickr.com/services/rest/?nojsoncallback=1&user_id=131866249%40N02
    #&method=flickr.people.getInfo&format=json
    (scheme, netloc, path, query, fragment) = urlparse.urlsplit(url)
    api_parameters = urlparse.parse_qs(query)
    assert "method" in api_parameters
    assert len(api_parameters["method"]) == 1
    api_method = api_parameters["method"][0]
    if api_method.startswith("flickr."):
        api_method = api_method[7:]
    #Remove method from api_args
    del api_parameters["method"]
    if "nojsoncallback" in api_parameters:
        del api_parameters["nojsoncallback"]
    if "format" in api_parameters:
        del api_parameters["format"]
    if "secret" in api_parameters:
        del api_parameters["secret"]

    return api_method, api_parameters


if __name__ == "__main__":
    #Logging
    logging.basicConfig(format='%(asctime)s: %(name)s --> %(message)s', level=logging.DEBUG)

    #Arguments
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    service_parser = subparsers.add_parser("service", help="Run harvesting service that consumes messages from "
                                                           "messaging queue.")
    service_parser.add_argument("host")
    service_parser.add_argument("username")
    service_parser.add_argument("password")

    seed_parser = subparsers.add_parser("seed", help="Harvest based on a seed file.")
    seed_parser.add_argument("filepath", help="Filepath of the seed file.")

    args = parser.parse_args()

    if args.command == "service":
        consumer = FlickrConsumer(host=args.host, username=args.username, password=args.password)
        consumer.consume()
    elif args.command == "seed":
        consumer = FlickrConsumer()
        with open(args.filepath) as seeds_file:
            seeds = seeds_file.read()
        resp = consumer.harvest_seeds(seeds)
        if resp:
            log.info("Result is: %s", resp)
            sys.exit(0)
        else:
            log.warning("Result is: %s", resp)
            sys.exit(1)
