#!/usr/bin/env python3

from __future__ import absolute_import
from sfmutils.warc_iter import BaseWarcIter
from datetime import datetime
import pytz
from urllib.parse import urlparse, parse_qs

TYPE_FLICKR_PHOTO = "flickr_photo"
TYPE_FLICKR_SIZES = "flickr_sizes"


class FlickrWarcIter(BaseWarcIter):
    def __init__(self, filepaths, limit_owner_nsids=None):
        BaseWarcIter.__init__(self, filepaths)
        self.limit_owner_nsids = limit_owner_nsids

    def _select_record(self, url):
        return url.startswith("https://api.flickr.com/services/rest/") \
               and ("method=flickr.photos.getInfo" in url or "method=flickr.photos.getSizes" in url)

    def _item_iter(self, url, json_obj):
        if "method=flickr.photos.getInfo" in url:
            if json_obj["stat"] == "ok":
                yield TYPE_FLICKR_PHOTO, json_obj["photo"]["id"], datetime.fromtimestamp(
                    int(json_obj["photo"]["dates"]["posted"]), tz=pytz.utc), json_obj["photo"]
            else:
                yield TYPE_FLICKR_PHOTO, None, None, None
        else:
            if json_obj["stat"] == "ok":
                # Need to split url to get photo id
                yield TYPE_FLICKR_SIZES, parse_qs(urlparse(url).query)["photo_id"][0], None, json_obj[
                    "sizes"]
            else:
                yield TYPE_FLICKR_SIZES, None, None, None

    @staticmethod
    def item_types():
        return [TYPE_FLICKR_PHOTO, TYPE_FLICKR_SIZES]

    def _select_item(self, item):
        if not self.limit_owner_nsids or item.get("owner", {}).get("nsid") in self.limit_owner_nsids:
            return True
        return False


if __name__ == "__main__":
    FlickrWarcIter.main(FlickrWarcIter)
