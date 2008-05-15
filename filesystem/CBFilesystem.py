from __future__ import with_statement

import fuse
import time
import stat
import threading
import cPickle
import UsedHashProvider
from array import array
from ChunkStoreManager import ChunkStoreManager


class CBFSStat(fuse.Stat):
	def __init__(self):
		self.st_mode = 0
		self.st_ino = 0
		self.st_dev = 0
		self.st_nlink = 1
		self.st_uid = 0
		self.st_gid = 0
		self.st_size = 0
		self.st_atime = time.time()
		self.st_mtime = time.time()
		self.st_ctime = time.time()

	def printme(self):
		print "mode: %o" % self.st_mode
		print "ino: %u" % self.st_ino
		print "dev: %u" % self.st_dev
		print "nlink: %u" % self.st_nlink
		print "uid: %u" % self.st_uid
		print "gid: %u" % self.st_gid
		print "size: %u" % self.st_size
		print "atime: %u" % self.st_atime
		print "mtime: %u" % self.st_mtime
		print "ctime: %u" % self.st_ctime


class CBFSDirentry(fuse.Direntry):

	parent = None
	st = None
	
	def __init__(self, name, parentdir):
		fuse.Direntry.__init__(self, name)
		self.parent = parentdir
		self.st = CBFSStat()
		

class CBFSDirectory(CBFSDirentry):

	entries = None
	
	def __init__(self, name, parentdir):
		CBFSDirentry.__init__(self, name, parentdir)
		if name == "": self.parent = self
		self.st.st_mode = stat.S_IFDIR | 0755
		self.entries = {}

	def get_used_hashes(self):
		m = {}
		for i in self.entries.values():
			m.update(i.get_used_hashes())
		return m
		

class CBFSFile(CBFSDirentry):

	## offset and length of data in first chunk
	#fcoffset = 0
	#fclength = 0
	## offset and length of data in last chunk
	#lcoffset = 0
	#lclength = 0

	hashes = None

	def __init__(self, name, parentdir):
		CBFSDirentry.__init__(self, name, parentdir)
		self.st.st_mode = stat.S_IFREG | 0444
		self.hashes = []

	def get_used_hashes(self):
		m = {}
		for i in self.hashes:
			m[i] = 1
		return m


class CBFSSymlink(CBFSDirentry):

	dest = None

	def __init__(self, name, parentdir, dest):
		CBFSDirentry.__init__(self, name, parentdir)
		self.dest = dest
		self.st.st_mode = stat.S_IFLNK | 0777

	def get_used_hashes(self):
		return {}


class CBFSDirtree(UsedHashProvider.UsedHashProvider):

	root = None
	# reserved space for hashes in functions load/save in bytes
	hashsize = ChunkStoreManager.hashsize
	# lock that *MUST* be acquired before directory tree is changed
	lock = None
	chunkstore = None	# *MUST* be set by external class
	indexhashes = None	# all hashes that are used by the filesystem index

	def __init__(self):
		self.root = CBFSDirectory("", None)
		self.lock = threading.RLock()
		self.indexhashes = []

	
	def getnode(self, name):
		dirnames = name.split("/")
		curdir = self.root

		if name == "" or name == "/":
			print "CBFSDirtree.getnode: returning root"
			return curdir

		for dir in dirnames:
			if dir == "" or dir == ".": 
				continue
			elif dir == "..": 
				curdir = curdir.parent
			elif curdir.entries.has_key(dir):
				curdir = curdir.entries[dir]
			else:
				raise IndexError, "path '%s' not found at item '%s'" % (name, dir) 

		return curdir
	

	def save(self):
		print "CBFSDirtree.save()"
		rootdump = cPickle.dumps(self.root)
		print "dirtree size: %d" % len(rootdump)
		bsize = self.chunkstore.chunksize-self.hashsize
		btodo = len(rootdump)
		nexthash = '\0'*self.hashsize
		while btodo>0:
			chunk = array('c', nexthash)
			bstart = max(0, btodo-bsize)
			print "bstart: %d, btodo: %d, len(rootdump): %d" % (bstart, btodo, len(rootdump))
			chunk.extend(array('c', rootdump[bstart:bstart+min(bsize, btodo)]))
			print "saving chunk with nexthash %s" % nexthash
			nexthash = self.chunkstore.put(chunk)
			btodo -= bsize

		print "initial chunk for dirtree: %s" % nexthash
		self.chunkstore.saveinithash(nexthash)
		return nexthash
			
	
	def load(self):
		print "CBFSDirtree.load()"
		hash = self.chunkstore.loadinithash()
		print "loading from hash %s" % hash
		self.indexhashes = []
		nexthash = hash
		rootdump = "" 
		zerohash = '\0'*self.hashsize
		while nexthash!=zerohash:
			print "loading hash '%s'" % nexthash
			self.indexhashes.append(nexthash)
			chunk = self.chunkstore.get(nexthash)
			nexthash = chunk[:self.hashsize].tostring()
			rootdump += chunk[self.hashsize:].tostring()
		print "dirtree size: %d" % len(rootdump)

		self.root = cPickle.loads(rootdump)
	

	def get_used_hashes(self):
		"returns a list of hashes which are currently used in filesystem"
		print "CBFSDirtree.get_used_hashes()"
		try:
			self.lock.acquire()
			h = self.root.get_used_hashes()
			for i in self.indexhashes:
				h[i] = 1
		finally:
			self.lock.release()
		return h
