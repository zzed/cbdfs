from __future__ import with_statement

import os
import fnmatch
from array import array 


class ChunkStoreServer:
	"implements a local chunk store that is accessed by ChunkStoreManager"

	chunksize = 2**23
	rootdir = "."
	hashalgo = None
	hashsize = None

	def __init__(self, directory, chunksize, hashalgo, hashsize):
		print "ChunkStoreServer.__init__(%s, %d)" % (directory, chunksize)
		self.chunksize = chunksize
		self.rootdir = directory
		self.hashalgo = hashalgo
		self.hashsize = hashsize
		self.thread_stopped = False
		if not os.path.exists(directory): 
			os.makedirs(directory)
		#self.shutdown = False
		#thread.start_new_thread(ChunkStore.background_thread, (self, ))

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
		assert(len(chunk)<=self.chunksize, "given chunk is larger than chunksize (%u)" % self.chunksize)

		# special case for zero-length chunks
		if len(chunk)==0:
			return "0";

		hash = self.hashalgo.copy()
		hash.update(chunk)
		d = hash.hexdigest()
		(fpath, fname) = self.__calcfilename(d)
		compname = "%s/%s/%s" % (self.rootdir, fpath, fname)
		if os.access(compname, os.F_OK|os.R_OK|os.W_OK):
			# file already exists, we don't need to do anything
			return d
		with open(compname, "w") as f:
			chunk.tofile(f)
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
			chunk.fromfile(f, min(s, self.chunksize))
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
		os.remove(compname)
		# TODO: remove paths which are unneeded

	
	def get_stored_hashes(self):
		files = []
		for file in os.listdir(self.rootdir):
			if fnmatch.fnmatch(file, '*.dat'):
				files.append(file[:-4])
		return files

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
