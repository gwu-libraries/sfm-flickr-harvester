from sfmutils.exporter import BaseExporter, BaseTable
from flickr_warc_iter import FlickrWarcIter, TYPE_FLICKR_PHOTO
import logging

log = logging.getLogger(__name__)

QUEUE = "flickr_exporter"
ROUTING_KEY = "export.start.flickr.flickr_user"


class FlickrPhotoTable(BaseTable):
    """
    PETL Table for Flickr photos.
    """

    def __init__(self, warc_paths, dedupe, item_date_start, item_date_end, seed_uids):
        BaseTable.__init__(self, warc_paths, dedupe, item_date_start, item_date_end, seed_uids, FlickrWarcIter,
                           limit_item_types=[TYPE_FLICKR_PHOTO])

    def _header_row(self):
        return ("photo_id", "date_posted", "date_taken", "license", "safety_level", "original_format", "owner_nsid",
                "owner_username", "title", "description", "media", "photopage")

    def _row(self, item):
        photopage_url = None
        for url in item["urls"]["url"]:
            if url["type"] == "photopage":
                photopage_url = url["_content"]
        return (item["id"], item["dates"]["posted"], item["dates"]["taken"], item["license"], item["safety_level"],
                item.get("originalformat"), item["owner"]["nsid"], item["owner"]["username"],
                item["title"]["_content"].replace('\n', ' '),
                item["description"]["_content"].replace('\n', ' '), item["media"], photopage_url)

    def id_field(self):
        return "photo_id"


class FlickrExporter(BaseExporter):
    def __init__(self, api_base_url, working_path, mq_config=None, warc_base_path=None):
        BaseExporter.__init__(self, api_base_url, FlickrWarcIter, FlickrPhotoTable, working_path,
                              mq_config=mq_config, warc_base_path=warc_base_path, limit_item_types=[TYPE_FLICKR_PHOTO])


if __name__ == "__main__":
    FlickrExporter.main(FlickrExporter, QUEUE, [ROUTING_KEY])
