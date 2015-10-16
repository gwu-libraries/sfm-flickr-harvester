import collections

class HarvestResponse():
    def __init__(self):
        self.success = True
        self.infos = []
        self.warnings = []
        self.errors = []
        self.urls = []
        self.summary = collections.Counter()

    def __nonzero__(self):
        return 1 if self.success else 0

    def __str__(self):
        harv_str = "Harvest response is %s." % self.success
        harv_str += self._str_messages(self.infos, "Informational")
        harv_str += self._str_messages(self.warnings, "Warning")
        harv_str += self._str_messages(self.errors, "Error")
        if self.urls:
            harv_str += " Urls: %s" % self.urls
        if self.summary:
            harv_str += " Harvest summary: %s" % self.summary
        return harv_str

    @staticmethod
    def _str_messages(messages, name):
        msg_str = ""
        if messages:
            msg_str += " %s messages are:" % name

        for (i, msg) in enumerate(messages, start=1):
            msg_str += "(%s) [%s] %s" % (i, msg["code"], msg["message"])

        return msg_str

    def merge(self, other):
        self.success = self.success and other.success
        self.infos.extend(other.infos)
        self.warnings.extend(other.warnings)
        self.errors.extend(other.errors)
        self.urls.extend(other.urls)
        self.summary.update(other.summary)

    def urls_as_set(self):
        return set(self.urls)

    def increment_summary(self, key):
        self.summary[key] += 1