from __future__ import absolute_import
import logging
import flickrapi
from sfmutils.harvester import BaseHarvester, Msg, CODE_TOKEN_NOT_FOUND, CODE_UID_NOT_FOUND, CODE_UNKNOWN_ERROR
from flickr_warc_iter import FlickrWarcIter, TYPE_FLICKR_PHOTO

log = logging.getLogger(__name__)

QUEUE = "flickr_harvester"
ROUTING_KEY = "harvest.start.flickr.*"


class FlickrHarvester(BaseHarvester):
    def __init__(self, working_path, mq_config=None, debug=False, per_page=None, debug_warcprox=False, tries=3):
        BaseHarvester.__init__(self, working_path, mq_config=mq_config, debug=debug, debug_warcprox=debug_warcprox,
                               tries=tries)
        self.api = None
        # For testing purposes
        self.per_page = per_page

    def harvest_seeds(self):
        # Create an API
        self._create_api()

        # Dispatch message based on type.
        harvest_type = self.message.get("type")
        log.debug("Harvest type is %s", harvest_type)
        if harvest_type == "flickr_user":
            self.users()
        else:
            raise KeyError

    def _create_api(self):
        self.api = flickrapi.FlickrAPI(self.message["credentials"]["key"],
                                       self.message["credentials"]["secret"],
                                       store_token=False)

    def users(self):
        # Options
        incremental = self.message.get("options", {}).get("incremental", True)

        for seed in self.message.get("seeds", []):
            self._user(seed.get("id"), seed.get("token"), seed.get("uid"), incremental)
            if not self.result.success:
                break

    def _user(self, seed_id, username, nsid, incremental):
        log.info("Harvesting user %s with seed_id %s. Incremental is %s.", username, seed_id, incremental)
        assert username or nsid
        # Lookup nsid
        if username and not nsid:
            nsid = self._lookup_nsid(username)
            if nsid:
                # Report back if nsid found
                self.result.uids[seed_id] = nsid
            else:
                msg = "NSID not found for user {}".format(username)
                log.exception(msg)
                self.result.warnings.append(Msg(CODE_TOKEN_NOT_FOUND, msg, seed_id=seed_id))
                return

        # Get info on the user
        resp = self.api.people.getInfo(user_id=nsid, format="parsed-json")
        if resp["stat"] != "ok":
            if resp["code"] == 1:
                msg = "NSID {} not found".format(nsid)
                log.warning(msg)
                self.result.warnings.append(Msg(CODE_UID_NOT_FOUND, msg, seed_id=seed_id))
            else:
                msg = "Error returned by API: {}".format(resp["message"])
                log.error(msg)
                self.result.errors.append(Msg(CODE_UNKNOWN_ERROR, msg))
                self.result.success = False
            return

        # Extract username
        new_username = resp["person"]["username"]["_content"]
        if new_username != username:
            self.result.token_updates[seed_id] = new_username

        page = 0
        # This is a dummy value. Will get actual value when call getPublicPhotos()
        total_pages = 1
        photo_ids = []
        while page < total_pages:
            page += 1
            log.debug("Fetching %s of %s pages.", page, total_pages)
            resp = self.api.people.getPublicPhotos(user_id=nsid, format='parsed-json', page=page,
                                                   per_page=self.per_page)
            # Get a valid value for total_pages
            total_pages = resp["photos"]["pages"]

            # Loop through photos on page
            for photo in resp["photos"]["photo"]:
                photo_ids.append((photo["id"], photo["secret"]))

        # Determine which photos should harvest
        to_harvest_photo_ids = photo_ids
        if incremental:
            last_photo_id = self.state_store.get_state(__name__, "{}.last_photo_id".format(nsid))
            if last_photo_id:
                # Photos are in most recently posted first order
                photo_id_subset = []
                for photo_id, secret in photo_ids:
                    if last_photo_id == photo_id:
                        break
                    photo_id_subset.append((photo_id, secret))
                to_harvest_photo_ids = photo_id_subset

        log.debug("Harvesting %s of %s photos", len(to_harvest_photo_ids), len(photo_ids))

        # Harvest photos
        for (photo_id, secret) in to_harvest_photo_ids:
            self._photo(photo_id, secret)
            if not self.result.success:
                break

    def _photo(self, photo_id, secret):
        log.info("Harvesting photo %s.", photo_id)

        # Get info
        self.api.photos.getInfo(photo_id=photo_id, secret=secret, format='parsed-json')

        # Get sizes
        self.api.photos.getSizes(photo_id=photo_id, format='parsed-json')

        self.result.harvest_counter["flickr photos"] += 1

    def _lookup_nsid(self, username):
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

    def process_warc(self, warc_filepath):
        sizes = self.message.get("options", {}).get("image_sizes", ("Thumbnail", "Large", "Original"))
        incremental = self.message.get("options", {}).get("incremental", True)

        warc_iter = FlickrWarcIter(warc_filepath)
        count = 0
        for item in warc_iter:
            if item.type == TYPE_FLICKR_PHOTO:
                count += 1
                if not count % 10:
                    log.debug("Processing %s photos", count)

                # Increment summary
                self.result.increment_stats("flickr photos")

                # Update state
                if incremental:
                    photo = item.item
                    self.state_store.set_state(__name__, "{}.last_photo_id".format(photo["owner"]["nsid"]), photo["id"])
            else:
                # Get sizes
                for size in item.item["size"]:
                    if size["label"] in sizes:
                        log.debug("Adding url for %s", size["label"])
                        self.result.urls.append(size["source"])
                    else:
                        log.debug("Skipping url for %s", size["label"])


if __name__ == "__main__":
    FlickrHarvester.main(FlickrHarvester, QUEUE, [ROUTING_KEY])
