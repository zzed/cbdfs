import socket
from cStringIO import StringIO

class CSProtClient:
    "manages the HTTP connection to ChunkStoreServers"

    host = None
    port = None
    _socket = None
    _c = None

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect()


    def connect(self):        
        self._socket.connect((self.host, self.port))
        self._c = self._socket.makefile()


    def put(self, chunk):
        body = "put\n"
        body += str(len(chunk.getvalue())) + "\n"
        self._c.write(body)
        self._c.write(chunk.getvalue())
        self._c.flush()
        hash = self._c.readline().rstrip()
        return hash


    def get(self, hash):
        print "CSProtClient.get(%s)" % hash
        self._c.write("get\n%s\n" % hash)
        self._c.flush()
        s = int(self._c.readline().rstrip())
        chunk = StringIO(self._c.read(s))
        return chunk


    def remove(self, hash):
        self._c.write("remove\n%s\n" % hash)
        self._c.flush()


    def get_stored_hashes(self):
        print "CSProtClient.get_stored_hashes"
        self._c.write("get_stored_hashes\n")
        self._c.flush()
        size = int(self._c.readline().rstrip())
        print "CSProtClient.get_stored_hashes: reading %d hashes" % size
        hashes = []
        for i in range(0,size):
            hashes.append(self._c.readline().rstrip())
        if len(hashes)>0 and hashes[-1]=="":
            hashes = hashes[:-1]
        return hashes
        
        
    def get_free_space(self):
        self._c.write("get_free_space\n")
        self._c.flush()
        size = self._c.readline().rstrip()
        return int(size)


    def get_used_space(self):
        self._c.write("get_used_space\n")
        self._c.flush()
        size = self._c.readline().rstrip()
        return int(size)


    def saveinithash(self, hash):
        self._c.write("save_init_hash\n%s\n" % hash)
        self._c.flush()


    def loadinithash(self):
        self._c.write("load_init_hash\n")
        self._c.flush()
        hash = self._c.readline().rstrip()
        return hash
