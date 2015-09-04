from __future__ import absolute_import
import tests
import tempfile
import os
import harvester.warc as sfh_warc
import warc as ia_warc
import unittest


@unittest.skipIf(tests.integration_env_available, "Skipping test since integration env is available.")
class TestWarcWriter(tests.TestCase):

    def setUp(self):
        self.warc_filepath = os.path.join(tempfile.mkdtemp(), "test.warc")

    def test_write(self):
        with sfh_warc.WarcWriterOpen(self.warc_filepath) as warc_writer:
            record = ia_warc.WARCRecord(payload="helloworld", headers={"WARC-Type": "response"})
            warc_writer.write_record(record)

        #Now read it back
        f = ia_warc.WARCFile(filename=self.warc_filepath)
        count = 0
        for r in f:
            count += 1
            self.assertEqual("response", r["WARC-Type"], "WARC-Type is not response.")
            self.assertEqual("helloworld", r.payload.read(), "Payload is not correct.")
        self.assertEqual(1, count, "WARC file does not contain 1 record.")

    def test_delete(self):
        #An empty (excluding warcinfo records) warc should be deleted when closed.
        with sfh_warc.WarcWriterOpen(self.warc_filepath) as warc_writer:
            record = ia_warc.WARCRecord(payload="helloworld", headers={"WARC-Type": "warcinfo"})
            warc_writer.write_record(record)

        self.assertFalse(os.path.exists(self.warc_filepath))