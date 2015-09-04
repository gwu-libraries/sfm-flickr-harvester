from __future__ import absolute_import
import logging
import os
import warc as ia_warc
from datetime import datetime

log = logging.getLogger(__name__)


def generate_warc_info(collection_path, collection_id, warc_type):
    """
    Generates the id and path for a WARC.

    :param collection_path: path of the collection
    :param collection_id: id of the collection
    :param warc_type: the type of content in the WARC. Primarily intended to
    make the id and path unique from other WARCs that may be created by other
    harvesters.
    :return: warc id, warc path, created_date
    """
    t = datetime.now()
    warc_id = "%s-%s-%s" % (collection_id, warc_type, t.strftime('%Y-%m-%dT%H:%M:%SZ'))
    warc_path = "%s/%s/%s/%s/%s/%s.warc.gz" % (
        collection_path,
        t.strftime('%Y'),
        t.strftime('%m'),
        t.strftime('%d'),
        t.strftime('%H'),
        warc_id,
    )
    return warc_id, warc_path, t


class WarcWriter():
    """
    A warc record writer that writes to a WARC file using the warc library.
    """
    def __init__(self, warc_path):
        """
        :param warc_path:  The path of the WARC file.
        """
        self._empty = True
        self.path = warc_path
        log.info("Writing to %s", self.path)

        #Create the directory
        filepath_parent = os.path.dirname(self.path)
        if not os.path.exists(filepath_parent):
            log.debug("Creating %s directory.", filepath_parent)
            os.makedirs(filepath_parent)

        #Open warc
        self._warc_file = ia_warc.open(self.path, "w")

    def close(self):
        """
        Close and delete if no warc records (excluding warcinfo records) have been written.
        """
        log.debug("Closing %s.", self.path)
        self._warc_file.close()

        if self._empty:
            log.debug("Deleting %s since empty.", self.path)
            os.remove(self.path)

    def write_record(self, warc_record):
        """
        :param warc_record:  The warc record to be written.  Should be type compatible with WARCRecord in the warc
        library.
        """
        self._warc_file.write_record(warc_record)
        if warc_record.type != "warcinfo":
            self._empty = False

    def write_records(self, warc_records):
        """
        :param warc_records:  The warc records to be written.
        """
        for warc_record in warc_records:
            self.write_record(warc_record)


class WarcWriterOpen():
    def __init__(self, warc_filepath):
        self.warc_filepath = warc_filepath
    def __enter__(self):
        self.warc_writer = WarcWriter(self.warc_filepath)
        return self.warc_writer
    def __exit__(self, type, value, traceback):
        self.warc_writer.close()