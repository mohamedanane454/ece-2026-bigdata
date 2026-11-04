import http.server, os
class H(http.server.SimpleHTTPRequestHandler):
    def do_PUT(self):
        p = self.translate_path(self.path)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        n = int(self.headers['Content-Length'])
        with open(p, 'wb') as f:
            f.write(self.rfile.read(n))
        self.send_response(201)
        self.end_headers()
    def end_headers(self):
        self.send_header('Cache-Control', 'no-store')
        super().end_headers()
os.chdir(os.path.expanduser('~/work/labkit'))
http.server.ThreadingHTTPServer(('127.0.0.1', 8099), H).serve_forever()
