import socket
from cStringIO import StringIO

class CSProtClient:
	"manages the HTTP connection to ChunkStoreServers"

	host = None
	port = None

	def __init__(self, host, port):
		self.host = host
		self.port = port


	def connect(self):
		conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		conn.connect((self.host, self.port))
		return conn.makefile()


	def put(self, chunk):
		c = self.connect()
		body = "put\n"
		body += str(len(chunk.getvalue())) + "\n"
		c.write(body)
		c.write(chunk.getvalue())
		c.flush()
		hash = c.readline().rstrip()
		return hash

	def get(self, hash):
		print "CSProtClient.get(%s)" % hash
		c = self.connect()
		c.write("get\n%s\n" % hash)
		c.flush()
		s = int(c.readline().rstrip())
		chunk = StringIO(c.read(s))
		return chunk

	def remove(self, hash):
		c = self.connect()
		c.write("remove\n%s\n" % hash)
		c.flush()

	def get_stored_hashes(self):
		print "CSProtClient.get_stored_hashes"
		c = self.connect()
		c.write("get_stored_hashes\n")
		c.flush()
		size = int(c.readline().rstrip())
		print "CSProtClient.get_stored_hashes: reading %d hashes" % size
		hashes = []
		for i in range(0,size):
			hashes.append(c.readline().rstrip())
		if len(hashes)>0 and hashes[-1]=="":
			hashes = hashes[:-1]
		return hashes
		
	def get_free_space(self):
		c = self.connect()
		c.write("get_free_space\n")
		c.flush()
		size = c.readline().rstrip()
		return int(size)


	def get_used_space(self):
		c = self.connect()
		c.write("get_used_space\n")
		c.flush()
		size = c.readline().rstrip()
		return int(size)


	def saveinithash(self, hash):
		c = self.connect()
		c.write("save_init_hash\n%s\n" % hash)
		c.flush()


	def loadinithash(self):
		c = self.connect()
		c.write("load_init_hash\n")
		c.flush()
		hash = c.readline().rstrip()
		return hash
