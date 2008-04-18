from __future__ import with_statement

import hashlib
import os
import copy
import thread
import time
import ChunkStoreServer
from array import array


class ChunkStoreManager:
	"stores chunks of varying size according to SHA1 hashes"

	chunksize = 2**23
	rootdir = "."
	hashalgo = hashlib.sha512()
	hashsize = 128
	usedhashprovider = None # should point to CBFSDirtree
	shutdown = None
	thread_stopped = None
	localchunks = None
	
	def __init__(self, usedhashprovider, directory = ".", chunksize = 2**23):
		print "ChunkStore.__init__(%s, %d)" % (directory, chunksize)
		self.usedhashprovider = usedhashprovider
		self.chunksize = chunksize
		self.rootdir = directory
		self.localchunks = ChunkStoreServer.ChunkStoreServer(directory, chunksize, self.hashalgo, self.hashsize)
		self.thread_stopped = False
		self.shutdown = False
		thread.start_new_thread(ChunkStoreManager.background_thread, (self, ))

	def stop(self):
		print "ChunkStore.stop()"
		self.shutdown = True
		while not self.thread_stopped:
			time.sleep(1)

	def put(self, chunk):
		print "ChunkStore.put(chunklen: %d)" % len(chunk)
		assert(len(chunk)<=self.chunksize, "given chunk is larger than chunksize (%u)" % self.chunksize)

		# special case for zero-length chunks
		if len(chunk)==0:
			return "0";

		return self.localchunks.put(chunk)


	def get(self, hashstring):
		# special case for zero-length chunks
		if hashstring=="0":
			return array('c')

		return self.localchunks.get(hashstring)
	

	def saveinithash(self, hash):
		self.localchunks.saveinithash(hash)


	def loadinithash(self):
		return self.localchunks.loadinithash()


	def do_chunk_gc(self):
		"performs chunk garbage collection and checks whether unused chunks are in use"
		print "ChunkStore.do_chunk_gc()"
		# collect all hashes stored here
		stored = self.localchunks.get_stored_hashes()
		used = self.usedhashprovider.get_used_hashes()
		if used is None: return
		c = 0
		for h in stored:
			if not h in used.keys(): 
				self.localchunks.remove(h)
				c += 1
		print "ChunkStoreManager: removed %d chunks during gc" % c


	def background_thread(self):
		try:
			initial_sleep = 2
			loop_sleep = 120
			print "Chunkstore.background_thread started, waiting %d seconds before operation starts ..." % initial_sleep
			time.sleep(initial_sleep)
			while not self.shutdown:
				self.do_chunk_gc()
				for i in range(1, loop_sleep):
					time.sleep(1)
					if self.shutdown: break 
			print "Chunkstore.background_thread shutting down"
		except:
			print "error in ChunkStore.background_thread"
			raise
		finally:
			self.thread_stopped = True
			

