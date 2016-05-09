from __future__ import absolute_import
import logging
import flickrapi
from sfmutils.harvester import BaseHarvester, Msg, CODE_TOKEN_NOT_FOUND, CODE_UID_NOT_FOUND, CODE_UNKNOWN_ERROR

log = logging.getLogger(__name__)

QUEUE = "flickr_harvester"
ROUTING_KEY = "harvest.start.flickr.*"


class FlickrHarvester(BaseHarvester):
    def __init__(self, mq_config=None, debug=False, per_page=None):
        BaseHarvester.__init__(self, mq_config=mq_config, debug=debug)
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
        options = self.message.get("options", {})
        incremental = options.get("incremental", True)
        sizes = options.get("sizes", ("Thumbnail", "Large", "Original"))

        for seed in self.message.get("seeds", []):
            self._user(seed.get("id"), seed.get("token"), seed.get("uid"), incremental, sizes)
            if not self.harvest_result.success:
                break

    def _user(self, seed_id, username, nsid, incremental, sizes):
        log.info("Harvesting user %s with seed_id %s. Incremental is %s. Sizes is %s", username, seed_id, incremental,
                 sizes)
        assert username or nsid
        # Lookup nsid
        if username and not nsid:
            nsid = self._lookup_nsid(username)
            if nsid:
                # Report back if nsid found
                self.harvest_result.uids[seed_id] = nsid
            else:
                msg = "NSID not found for user {}".format(username)
                log.exception(msg)
                self.harvest_result.warnings.append(Msg(CODE_TOKEN_NOT_FOUND, msg))
                return

        # Get info on the user
        resp = self.api.people.getInfo(user_id=nsid, format="parsed-json")
        if resp["stat"] != "ok":
            if resp["code"] == 1:
                msg = "NSID {} not found".format(nsid)
                log.warning(msg)
                self.harvest_result.warnings.append(Msg(CODE_UID_NOT_FOUND, msg))
            else:
                msg = "Error returned by API: {}".format(resp["message"])
                log.error(msg)
                self.harvest_result.errors.append(Msg(CODE_UNKNOWN_ERROR, msg))
                self.harvest_result.success = False
            return

        # Extract username
        new_username = resp["person"]["username"]["_content"]
        if new_username != username:
            self.harvest_result.token_updates[seed_id] = new_username

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
                photo_id_subset = []
                found_last_photo_id = False
                for photo_id, secret in photo_ids:
                    if found_last_photo_id:
                        photo_id_subset.append((photo_id, secret))
                    if last_photo_id == photo_id:
                        found_last_photo_id = True
                if found_last_photo_id:
                    to_harvest_photo_ids = photo_id_subset

        log.debug("Harvesting %s of %s photos", len(to_harvest_photo_ids), len(photo_ids))

        # Harvest photos
        new_last_photo_id = None
        for (photo_id, secret) in to_harvest_photo_ids:
            self._photo(photo_id, secret, sizes)
            if not self.harvest_result.success:
                break
            new_last_photo_id = photo_id

        # Save new last photo id for future incremental
        if new_last_photo_id:
            log.debug("New last photo id is %s", new_last_photo_id)
            self.state_store.set_state(__name__, "{}.last_photo_id".format(nsid), new_last_photo_id)

    def _photo(self, photo_id, secret, sizes):
        log.info("Harvesting photo %s. Sizes is %s", photo_id, sizes)

        # Get info
        self.api.photos.getInfo(photo_id=photo_id, secret=secret, format='parsed-json')

        # Get sizes
        resp = self.api.photos.getSizes(photo_id=photo_id, format='parsed-json')
        for size in resp["sizes"]["size"]:
            if size["label"] in sizes:
                log.debug("Adding url for %s", size["label"])
                self.harvest_result.urls.append(size["source"])
            else:
                log.debug("Skipping url for %s", size["label"])

        # Increment summary
        self.harvest_result.increment_summary("Flickr photo")

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


if __name__ == "__main__":
    FlickrHarvester.main(FlickrHarvester, QUEUE, [ROUTING_KEY])
