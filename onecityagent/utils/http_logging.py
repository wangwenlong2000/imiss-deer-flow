"""HTTP access logs without prompts, artifact query URLs or credentials."""

from urllib.parse import urlsplit

from werkzeug.serving import WSGIRequestHandler


class PathOnlyRequestHandler(WSGIRequestHandler):
    def log_request(self, code="-", size="-"):
        self.log(
            "info", '"%s %s %s" %s %s',
            self.command, urlsplit(self.path).path, self.request_version, code, size,
        )
