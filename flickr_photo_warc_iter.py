#!/usr/bin/env python

from __future__ import absolute_import
from sfmutils.warc_iter import BaseWarcIter
from datetime import datetime


class FlickrPhotoWarcIter(BaseWarcIter):
    def __init__(self, filepaths, limit_owner_nsids=None):
        BaseWarcIter.__init__(self, filepaths)
        self.limit_owner_nsids = limit_owner_nsids

    def _select_record(self, url):
        return url.startswith("https://api.flickr.com/services/rest/") and "method=flickr.photos.getInfo" in url

    def _item_iter(self, url, json_obj):
        if json_obj["stat"] == "ok":
            yield "flickr_photo", json_obj["photo"]["id"], datetime.utcfromtimestamp(
                int(json_obj["photo"]["dates"]["posted"])), json_obj["photo"]
        else:
            yield "flickr_photo", None, None, None

    @staticmethod
    def item_types():
        return ["flickr_photo"]

    def _select_item(self, item):
        if not self.limit_owner_nsids or item.get("owner", {}).get("nsid") in self.limit_owner_nsids:
            return True
        return False

if __name__ == "__main__":
    FlickrPhotoWarcIter.main(FlickrPhotoWarcIter)
