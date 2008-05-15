import SocketServer
import pdb
from array import array

class CSProtServer(SocketServer.StreamRequestHandler):
	"manages communication with ChunkStoreManager by offering a HTTP server"	

	csserver = None  # set by main method
	allowed_path = "/ChunkStoreServer"


	def handle(self):
		cmd = self.rfile.readline();
		cmd = cmd.rstrip()
		print "cmd: '%s'" % cmd

		try:
			if cmd=="get_stored_hashes":
				h = self.csserver.get_stored_hashes()
				self.wfile.write("%s\n" % len(h))
				for i in h:
					self.wfile.write(i + "\n")
			elif cmd=="get_free_space":
				self.wfile.write(str(self.csserver.get_free_space()) + "\n")
			elif cmd=="put":
				size = self.rfile.readline().rstrip()
				print "CSProtServer.do_GET: size: %s" % size
				size = int(size)
				cstr = self.rfile.read(size)
				chunk = array('c', cstr)
				hash = self.csserver.put(chunk)
				self.wfile.write(hash + "\n")
			elif cmd=="get":
				hash = self.rfile.readline().rstrip()
				chunk = self.csserver.get(hash)
				self.wfile.write("%d\n" % len(chunk))
				self.wfile.write(chunk.tostring())
			elif cmd=="remove":
				hash = self.rfile.readline().rstrip()
				self.csserver.remove(hash)
			elif cmd=="save_init_hash":
				hash = self.rfile.readline().rstrip()
				self.csserver.saveinithash(hash)
			elif cmd=="load_init_hash":
				self.wfile.write(self.csserver.loadinithash() + "\n")
		except:
			raise
