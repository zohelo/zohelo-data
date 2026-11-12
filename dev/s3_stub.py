"""A local S3 stub: per (method, key) it answers a scripted list of throttle answers, then succeeds."""
from collections import Counter, defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import threading

ERR = {
    "429": (429, b""),  # HEAD has no body: botocore sees code "429"
    "429body": (429, b"<Error><Code>TooManyRequests</Code><Message>Too Many Requests</Message></Error>"),
    "slowdown": (503, b"<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>"),
    "unavailable": (503, b"<Error><Code>ServiceUnavailable</Code><Message>Reduce your rate of simultaneous reads on the same object.</Message></Error>"),
    "denied": (403, b"<Error><Code>AccessDenied</Code><Message>no</Message></Error>"),
}


class Stub:
    def __init__(self):
        self.script = defaultdict(list)  # (method, path?query-kind) -> list of error names, consumed in order
        self.always = {}  # method -> error name for every request (sustained throttling)
        self.objects = {}  # key -> (bytes, metadata)
        self.calls = Counter()
        self.puts = Counter()  # completed writes per key
        self.uploads = {}
        self.lock = threading.Lock()
        stub = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _send(self, status, body=b"", headers=None, head=False):
                self.send_response(status)
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                if not head or "Content-Length" not in (headers or {}):
                    self.send_header("Content-Length", str(len(body)))
                if body and not head:
                    self.send_header("Content-Type", "application/xml")
                self.end_headers()
                if not head:
                    self.wfile.write(body)

            def _body(self):
                if self.headers.get("Transfer-Encoding", "").lower() == "chunked" or "aws-chunked" in self.headers.get("Content-Encoding", ""):
                    raw = self.rfile.read(int(self.headers.get("Content-Length", 0))) if self.headers.get("Content-Length") else b""
                    if not raw:  # real chunked transfer
                        out = b""
                        while True:
                            size = int(self.rfile.readline().split(b";")[0], 16)
                            if size == 0:
                                while self.rfile.readline().strip():
                                    pass
                                return out
                            out += self.rfile.read(size)
                            self.rfile.readline()
                    # aws-chunked with content-length: decode chunks
                    out, pos = b"", 0
                    while pos < len(raw):
                        end = raw.index(b"\r\n", pos)
                        size = int(raw[pos:end].split(b";")[0], 16)
                        if size == 0:
                            break
                        out += raw[end + 2 : end + 2 + size]
                        pos = end + 2 + size + 2
                    return out
                return self.rfile.read(int(self.headers.get("Content-Length", 0)))

            def _handle(self, method):
                path, _, query = self.path.partition("?")
                kind = method
                if method == "GET" and "list-type" in query:
                    kind = "LIST"
                elif method == "POST" and "uploads" in query and "uploadId" not in query:
                    kind = "CREATE_MPU"
                elif method == "PUT" and "partNumber" in query:
                    kind = "PART"
                elif method == "POST" and "uploadId" in query:
                    kind = "COMPLETE_MPU"
                elif method == "POST" and "delete" in query:
                    kind = "DELETE_MANY"
                body = self._body() if method in ("PUT", "POST") else b""
                with stub.lock:
                    stub.calls[kind] += 1
                    queue = stub.script[kind]
                    name = queue.pop(0) if queue else stub.always.get(kind)
                if name:
                    status, payload = ERR[name]
                    return self._send(status, payload, head=method == "HEAD")
                key = path.split("/", 2)[2] if path.count("/") >= 2 else ""
                if kind == "LIST":
                    items = "".join(f"<Contents><Key>{k}</Key><Size>{len(v[0])}</Size></Contents>" for k, v in sorted(stub.objects.items()))
                    return self._send(200, f"<ListBucketResult><IsTruncated>false</IsTruncated>{items}</ListBucketResult>".encode())
                if kind == "CREATE_MPU":
                    stub.uploads[key] = {}
                    return self._send(200, f"<InitiateMultipartUploadResult><UploadId>u1</UploadId><Key>{key}</Key></InitiateMultipartUploadResult>".encode())
                if kind == "PART":
                    n = int(query.split("partNumber=")[1].split("&")[0])
                    stub.uploads[key][n] = body
                    return self._send(200, headers={"ETag": f'"{hashlib.md5(body).hexdigest()}"'})
                if kind == "COMPLETE_MPU":
                    parts = stub.uploads.pop(key)
                    with stub.lock:
                        stub.objects[key] = (b"".join(parts[n] for n in sorted(parts)), dict(stub.meta.pop(key, {})))
                        stub.puts[key] += 1
                    return self._send(200, f"<CompleteMultipartUploadResult><Key>{key}</Key><ETag>\"x\"</ETag></CompleteMultipartUploadResult>".encode())
                if kind == "DELETE_MANY":
                    return self._send(200, b"<DeleteResult></DeleteResult>")
                if method == "PUT":
                    meta = {k[11:]: v for k, v in self.headers.items() if k.lower().startswith("x-amz-meta-")}
                    with stub.lock:
                        stub.objects[key] = (body, meta)
                        stub.puts[key] += 1
                    return self._send(200, headers={"ETag": f'"{hashlib.md5(body).hexdigest()}"'})
                if method == "DELETE":
                    stub.objects.pop(key, None)
                    return self._send(204)
                if key not in stub.objects:
                    return self._send(404, b"" if method == "HEAD" else b"<Error><Code>NoSuchKey</Code><Message>x</Message></Error>", head=method == "HEAD")
                data, meta = stub.objects[key]
                headers = {"Content-Length": str(len(data)), "ETag": '"e"', **{f"x-amz-meta-{k}": v for k, v in meta.items()}}
                if method == "HEAD":
                    return self._send(200, headers=headers, head=True)
                self.send_response(200)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                self._handle("GET")

            def do_HEAD(self):
                self._handle("HEAD")

            def do_PUT(self):
                self._handle("PUT")

            def do_POST(self):
                self._handle("POST")

            def do_DELETE(self):
                self._handle("DELETE")

        self.meta = {}
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
