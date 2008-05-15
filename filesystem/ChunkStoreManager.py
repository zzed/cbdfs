from __future__ import with_statement

import hashlib
import os
import copy
import thread
import time
import traceback
import ConfigParser
import CSProtClient
from array import array


class ChunkStore:
	"represents a ChunkStoreServer and caches some local variables"

	stored_hashes = None
	_free_space = None
	_maxSpace = None
	_client = None
	available = False
	host = None
	port = None

	def __init__(self, host, port):
		print "ChunkStore.__init__(%s, %d)" % (host, port)
		self.host = host
		self.port = port
		self._client = CSProtClient.CSProtClient(host, port)
		self.available = True
		try:
			self.stored_hashes = self._client.get_stored_hashes()
			self._updateSpace()
			print "ChunkStore.__init__: initialized client '%s', free space: %d, stored chunks: %d" % (host, self._free_space, len(self.stored_hashes))
			print "stored hashes: " + str(self.stored_hashes)
		except:
			print "ChunkStore.__init__: setting store %s to non-available" % self.host
			self.available = False
			raise
	
	def remove(self, hash):
		self._client.remove(hash)
		self.stored_hashes.remove(hash)

	def put(self, chunk):
		hash = self._client.put(chunk)
		if not hash in self.stored_hashes:
			self.stored_hashes.append(hash)
			self._free_space -= len(chunk)
		return hash

	def get(self, hash):
		return self._client.get(hash)

	def getFreeSpace(self):
		return self._free_space

	def getUsedSpace(self):
		return self._maxSpace-self._free_space
	
	def _updateSpace(self):
		self._free_space = self._client.get_free_space()
		used_space = self._client.get_used_space()
		self._maxSpace = self._free_space+used_space

	def getStoredHashes(self):
		return self._client.get_stored_hashes()

	def saveInitHash(self, hash):
		return self._client.saveinithash(hash)

	def loadInitHash(self):
		return self._client.loadinithash()



class ChunkStoreManager:
	"stores chunks of varying size according to SHA1 hashes"

	chunksize = None
	hashalgo = hashlib.sha512()
	hashsize = 128
	usedhashprovider = None # should point to CBFSDirtree
	shutdown = None
	thread_stopped = None
	chunkstores = None

	# commands to be executed immediately by background thread
	do_gc = None

	
	def __init__(self, usedhashprovider, config):
		print "ChunkStore.__init__(%s)" % (config)
		self.usedhashprovider = usedhashprovider
		self.chunkstores = []
		if config is not None:
			self.__load_config(config)
		self.thread_stopped = False
		self.do_gc = False
		self.shutdown = False
		thread.start_new_thread(ChunkStoreManager.background_thread, (self, ))


	def __load_config(self, configfile):
		print "ChunkStoreManager.__load_config(%s)" % configfile
		section = "ChunkStoreManager"
		cp = ConfigParser.ConfigParser()
		cp.read(configfile)
		self.chunksize = cp.getint(section, "chunksize")
		self.chunkstores = []
		count = 1
		try:
			while True:
				host = cp.get(section, "storehost_%d" % count)
				port = cp.getint(section, "storeport_%d" % count)
				self.chunkstores.append(ChunkStore(host, port))
				count += 1
		except:
			traceback.print_exc()
		if count==1:
			raise Exception("no store host found!")


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

		h = self.hashalgo.copy()
		h.update(chunk)
		hash = h.hexdigest()

		# look if chunk is already stored
		for cs in self.chunkstores:
			if cs.available and hash in cs.stored_hashes:
				return hash
				
		
		while True:
			# select some chunk store which has most available space
			maxcs = None
			for cs in self.chunkstores:
				print "name: %s, available: %d" % (cs.host, cs.available)
				if cs.available:
					if maxcs is None or cs.getFreeSpace()>maxcs.getFreeSpace():
						maxcs = cs
			if maxcs is None:
				raise Exception, "ChunkStoreManager.put: ERROR: failed to find free available chunk store!"

			# now save chunk
			try:
				hash = maxcs.put(chunk)
				maxcs.stored_hashes.append(hash)
				break
			except:
				print "ChunkStore.put: setting store %s to non-available" % maxcs.host
				maxcs.available = False
				traceback.print_exc()
				print "ChunkStoreManager.put: failed to store chunk to store %s" % cs.host
			
		return hash


	def get(self, hashstring):
		# special case for zero-length chunks
		if hashstring=="0":
			return array('c')

		while True:
			# where is chunk stored?
			chunkstore = None
			for cs in self.chunkstores:
				if cs.available and hashstring in cs.stored_hashes:
					chunkstore = cs
					break
			if chunkstore is None:
				raise Exception("ChunkStoreManager.get: failed to find requested chunk with hash '%s'" % hashstring)

			try:
				chunk = chunkstore.get(hashstring)
				return chunk
			except:
				traceback.print_exc()
				print "ChunkStore.get: setting store %s to non-available" % chunkstore.host
				chunkstore.available = False
				print "ChunkStoreManager.get: failed to read chunk from store %s" % chunkstore.host

		return None
	

	def saveinithash(self, hash):
		for cs in self.chunkstores:
			if cs.available:
				try:
					cs.saveInitHash(hash)
				except:
					print "ChunkStore.saveinithash: setting store %s to non-available" % cs.host
					cs.available = False
					traceback.print_exc()
					print "ChunkStoreManager.saveinithash: failed to save inithash to store %s" % cs.host


	def loadinithash(self):
		for cs in self.chunkstores:
			if cs.available:
				try:
					hash = cs.loadInitHash()
					return hash
				except:
					traceback.print_exc()
					print "ChunkStoreManager.loadinithash: failed to load inithash from store %s" % cs.host
		raise Exception("failed to retrieve init hash (no stores are available!)!")


	def _do_chunk_gc(self):
		"performs chunk garbage collection and checks whether unused chunks are in use"
		print "ChunkStore.do_chunk_gc()"
		# collect all hashes used by filesystem
		usedhashes = self.usedhashprovider.get_used_hashes()
			
		# FIXME: here we can get a race condition that results in lost data:
		# assume that chunk A existed in chunkstores, but was deleted
		# here it will be removed then. But what happens if it was created in the fs again?
		# -> chunk will still be deleted, but is needed!

		# remove all unused hashes
		for cs in self.chunkstores:
			if cs.available:
				count = 0
				for h in cs.stored_hashes:
					if not h in usedhashes:
						cs.remove(h)
						count += 1
				print "ChunkStoreManager.do_chunk_gc: removed %d chunks at host %s" % (count, cs.host)

		# TODO: look if all chunks have enough duplicates


	def getSpaceStat(self):
		"returns tuple containing (currently used space, maximum space) of filesystem"
		maxsize = 0
		cursize = 0
		for cs in self.chunkstores:
			if cs.available:
				cursize += cs.getUsedSpace()
				maxsize += cursize + cs.getFreeSpace()
		return (cursize, maxsize)


	def background_thread(self):
		try:
			initial_sleep = 2
			loop_sleep = 120
			print "Chunkstore.background_thread started, waiting %d seconds before operation starts ..." % initial_sleep
			time.sleep(initial_sleep)
			while not self.shutdown:
				self._do_chunk_gc()
				for i in range(1, loop_sleep):
					time.sleep(1)
					if self.shutdown: break 
					if self.do_gc:
						self._do_chunk_gc()
						self.do_gc = False
			print "Chunkstore.background_thread shutting down"
		except:
			print "error in ChunkStore.background_thread"
			raise
		finally:
			self.thread_stopped = True
			

