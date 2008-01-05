from __future__ import with_statement

import hashlib
import os
from array import array


class ChunkStore:
	"stores chunks of varying size according to SHA1 hashes"

	chunksize = 2**23
	rootdir = "."
	hash = hashlib.sha512()
	
	def __init__(self, directory = ".", chunksize = 2**23):
		self.chunksize = chunksize
		self.rootdir = directory
		if not os.path.exists(directory): 
			os.makedirs(directory)

	# calculates the path and filename of chunk file
	def __calcfilename(self, hexdigest):
		return (".", "%s.dat" % hexdigest)

	def put(self, chunk):
		assert(len(chunk)<=self.chunksize, "given chunk is larger than chunksize (%u)" % self.chunksize)

		self.hash.update(chunk)
		d = self.hash.hexdigest()
		(fpath, fname) = self.__calcfilename(d)
		compname = "%s/%s/%s" % (self.rootdir, fpath, fname)
		if os.access(compname, os.F_OK|os.R_OK|os.W_OK):
			# file already exists, we don't need to do anything
			return d
		with open(compname, "w") as f:
			chunk.tofile(f)
			#f.write(chunk)
		return d


	def get(self, hashstring):
		(fpath, fname) = self.__calcfilename(hashstring)
		compname = "%s/%s/%s" % (self.rootdir, fpath, fname)
		assert(os.access(compname, os.F_OK|os.R_OK), "failed to access file %s" % compname)
		s = os.stat(compname).st_size
		with open(compname, "r") as f:
			chunk = array('c')
			chunk.fromfile(f, min(s, self.chunksize))
		self.hash.update(chunk)
		d = self.hash.hexdigest()
		assert(d==hashstring, "file %s does not have correct checksum %s" % (compname, hashstring))

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
		compname = "%s/%s/%s" % (self.directory, fpath, fname)
		os.remove(compname)
		# TODO: remove paths which are unneeded

	def check(self):
		# TODO: check all stored chunks if hashes are correct
		pass
