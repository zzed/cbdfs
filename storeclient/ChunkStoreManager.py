from __future__ import with_statement

import hashlib
import os
import copy
import thread
import time
import traceback
import ConfigParser
import ChunkStoreServer
import CSProtClient
from array import array


class ChunkStore:
	"represents a ChunkStoreServer"

	stored_hashes = None
	free_space = None
	client = None
	available = False
	host = None
	port = None

	def __init__(self, host, port):
		print "ChunkStore.__init__(%s, %d)" % (host, port)
		self.host = host
		self.port = port
		self.client = CSProtClient.CSProtClient(host, port)
		self.available = True
		try:
			self.stored_hashes = self.client.get_stored_hashes()
			self.free_space = self.client.get_free_space()
			print "ChunkStore.__init__: initialized client '%s', free space: %d, stored chunks: %d" % (host, self.free_space, len(self.stored_hashes))
			print "stored hashes: " + str(self.stored_hashes)
		except:
			print "ChunkStore.__init__: setting store %s to non-available" % self.host
			self.available = False
			raise
	


class ChunkStoreManager:
	"stores chunks of varying size according to SHA1 hashes"

	chunksize = None
	hashalgo = hashlib.sha512()
	hashsize = 128
	usedhashprovider = None # should point to CBFSDirtree
	shutdown = None
	thread_stopped = None
	chunkstores = None

	
	def __init__(self, usedhashprovider, config):
		print "ChunkStore.__init__(%s)" % (config)
		self.usedhashprovider = usedhashprovider
		self.chunkstores = []
		if config is not None:
			self.__load_config(config)
		self.thread_stopped = False
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
					if maxcs is None or cs.free_space>maxcs.free_space:
						maxcs = cs
			if maxcs is None:
				raise Exception, "ChunkStoreManager.put: ERROR: failed to find free available chunk store!"

			# now save chunk
			try:
				hash = maxcs.client.put(chunk)
				maxcs.stored_hashes.append(hash)
				maxcs.free_space
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
				chunk = chunkstore.client.get(hashstring)
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
					cs.client.saveinithash(hash)
				except:
					print "ChunkStore.saveinithash: setting store %s to non-available" % cs.host
					cs.available = False
					traceback.print_exc()
					print "ChunkStoreManager.saveinithash: failed to save inithash to store %s" % cs.host


	def loadinithash(self):
		for cs in self.chunkstores:
			if cs.available:
				try:
					hash = cs.client.loadinithash()
					return hash
				except:
					traceback.print_exc()
					print "ChunkStoreManager.loadinithash: failed to load inithash from store %s" % cs.host
		raise Exception("failed to retrieve init hash (no stores are available!)!")


	def do_chunk_gc(self):
		"performs chunk garbage collection and checks whether unused chunks are in use"
		print "ChunkStore.do_chunk_gc()"
		return
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
			

