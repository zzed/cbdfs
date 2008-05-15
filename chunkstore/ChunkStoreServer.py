from __future__ import with_statement

import os
import fnmatch
import hashlib
import sys
import ConfigParser
import BaseHTTPServer
import SocketServer
from array import array 

import CSProtServer


class ChunkStoreServer:
	"implements a local chunk store that is accessed by ChunkStoreManager"

	rootdir = "."
	hashalgo = hashlib.sha512()
	hashsize = 128
	maxspace = None
	curspace = None

	def __init__(self, rootdir, maxspace):
		print "ChunkStoreServer.__init__(%s, %s)" % (rootdir, maxspace)
		self.maxspace = maxspace
		self.rootdir = rootdir
		self.thread_stopped = False
		if not os.path.exists(self.rootdir): 
			os.makedirs(self.rootdir)
		self.get_free_space()


	def stop(self):
		print "ChunkStoreServer.stop()"
		#self.shutdown = True
		#while not self.thread_stopped:
			#time.sleep(1)

	# calculates the path and filename of chunk file
	def __calcfilename(self, hexdigest):
		return (".", "%s.dat" % hexdigest)

	def put(self, chunk):
		print "ChunkStore.put(chunklen: %d)" % len(chunk)

		# special case for zero-length chunks
		if len(chunk)==0:
			return "0";

		hash = self.hashalgo.copy()
		hash.update(chunk)
		d = hash.hexdigest()
		print "ChunkStore.put: hash '%s'" % d
		(fpath, fname) = self.__calcfilename(d)
		compname = "%s/%s/%s" % (self.rootdir, fpath, fname)
		print "compname: " + compname
		if os.access(compname, os.F_OK|os.R_OK|os.W_OK):
			# file already exists, we don't need to do anything
			print "ChunkStore.put: file already exists"
			return d
		with open(compname, "w") as f:
			chunk.tofile(f)
			self.curspace += len(chunk)
		return d


	def get(self, hashstring):
		# special case for zero-length chunks
		if hashstring=="0":
			return array('c')

		(fpath, fname) = self.__calcfilename(hashstring)
		compname = "%s/%s/%s" % (self.rootdir, fpath, fname)
		assert(os.access(compname, os.F_OK|os.R_OK), "failed to access file %s" % compname)
		s = os.stat(compname).st_size
		with open(compname, "r") as f:
			chunk = array('c')
			chunk.fromfile(f, s)
		hash = self.hashalgo.copy()
		hash.update(chunk)
		d = hash.hexdigest()
		assert(d==hashstring, "file %s does not have correct checksum %s" % (compname, hashstring))

		print "returning chunk for hash %s" % hashstring
		return chunk
	

	def saveinithash(self, hash):
		with open("%s/inithash" % self.rootdir, "w") as f:
			f.truncate(0)
			f.write(hash)


	def loadinithash(self):
		with open("%s/inithash" % self.rootdir, "r") as f:
			return f.readline()


	def remove(self, hashstring):
		(fpath, fname) = self.__calcfilename(hashstring)
		compname = "%s/%s/%s" % (self.rootdir, fpath, fname)
		self.curspace -= os.stat(compname).st_size
		os.remove(compname)
		# TODO: remove paths which are unneeded

	
	def get_stored_hashes(self):
		files = []
		for file in os.listdir(self.rootdir):
			if fnmatch.fnmatch(file, '*.dat'):
				files.append(file[:-4])
		print "get_stored_hashes: returning list %s" % files
		return files

	
	def get_free_space(self):
		if self.curspace is None:
			self.curspace = 0
			for file in os.listdir(self.rootdir):
				if fnmatch.fnmatch(file, '*.dat'):
					self.curspace += os.stat("%s/%s" % (self.rootdir, file)).st_size
		print "ChunkStoreServer.get_free_space: %d" % (self.maxspace-self.curspace)
		return self.maxspace-self.curspace

	#def background_thread(self):
		#try:
			#initial_sleep = 5
			#loop_sleep = 120
			#print "Chunkstore.background_thread started, waiting %d seconds before operation starts ..." % initial_sleep
			#time.sleep(initial_sleep)
			#while not self.shutdown:
				#do_chunk_gc()
				#for i in range(1, loop_sleep):
					#time.sleep(1)
					#if self.shutdown: break 
			#print "Chunkstore.background_thread shutting down"
		#except:
			#print "error in ChunkStore.background_thread"
			#raise
		#finally:
			#self.thread_stopped = True

def load_config(configfile):
	section = "ChunkStoreServer"
	cp = ConfigParser.ConfigParser()
	cp.read(configfile)
	rootdir = cp.get(section, "chunkdir")
	s = cp.get(section, "maxspace")
	s = s.replace("MB", "")
	maxspace = int(s)*1024*1024
	port = cp.getint(section, "port")
	host = cp.get(section, "hostname")
	return (rootdir, maxspace, port, host)


# main execution
if __name__ == '__main__':
	print "server starting ..."

	if len(sys.argv)!=2:
		print "usage: <command> <configfile>"
		sys.exit(1)

	(rootdir, maxspace, port, host) = load_config(sys.argv[1])

	csserver = ChunkStoreServer(rootdir, maxspace)
	CSProtServer.CSProtServer.csserver = csserver
	dispatcher = SocketServer.ForkingTCPServer(('', port), CSProtServer.CSProtServer)
	print 'listening on %d ...' % port
	
	try:
		dispatcher.serve_forever()
	except KeyboardInterrupt:
		pass
	dispatcher.server_close()
