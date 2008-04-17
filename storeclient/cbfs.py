#!/usr/bin/env python

import os, sys, errno, stat, copy, cPickle, traceback, time
from chunkstore import ChunkStore
from array import array

sys.path.append("fuse-build")
import fuse
import traceback
from fuse import Fuse


if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."

fuse.fuse_python_api = (0, 2)


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


class CBFSSymlink(CBFSDirentry):

	dest = None

	def __init__(self, name, parentdir, dest):
		CBFSDirentry.__init__(self, name, parentdir)
		self.dest = dest



class CBFSDirtree:

	root = None
	# reserved space for hashes in functions load/save in bytes
	hashsize = 200   

	def __init__(self):
		self.root = CBFSDirectory("", None)

	
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
		bleft = len(rootdump)
		bsize = chunkstore.chunksize-self.hashsize
		nexthash = '\0'*self.hashsize
		while bleft>0:
			bstart = max(bleft-bsize, 0)
			chunk = array('c', '\0' * self.hashsize)
			bend = min(bstart+bsize, len(rootdump))
			chunk.extend(array('c', rootdump[bstart:bstart+bsize]))
			chunk[:self.hashsize] = array('c', nexthash)
			nexthash = chunkstore.put(chunk)
			bleft -= bsize
		return nexthash
			
	
	def load(self, hash):
		print "CBFSDirtree.load(%s)" % hash
		nexthash = hash
		rootdump = "" 
		zerohash = '\0'*self.hashsize
		while nexthash!=zerohash:
			chunk = chunkstore.get(hash)
			nexthash = chunk[:self.hashsize].tostring()
			rootdump += chunk[self.hashsize:].tostring()
		print "dirtree size: %d" % len(rootdump)
		self.root = cPickle.loads(rootdump)
			



class CBFSFilehandle(object):

	# all operations on chunks is done on those variables only,
	# they are loaded by the internal functions __loadchunk and 
	# written by __writechunk
	actchunk = None
	actcidx = -1
	actcmodified = False
	# file-wise FUSE attributes
	direct_io = False  
	keep_cache = False


	def __init__(self, path, flags, *mode):
		print "CBFSFilehandle.__init__(%s, %s, %s)" % (path, flags, mode)
		try:
			self.node = dirtree.getnode(path)
		except:
			if flags&os.O_CREAT==0: 
				print "no O_CREATE flag set"
				return -errno.ENOENT
			# create new file
			(pdname, sep, fn) = path.rpartition("/")
			if pdname == "": pdname = "/"
			try:
				pdir = dirtree.getnode(pdname)
			except:
				print "parent dir not found"
				return -errno.ENOENT
			if not isinstance(pdir, CBFSDirectory):
				print "parent dir is no directory"
				return -errno.EACCES
			self.node = pdir.entries[fn] = CBFSFile(fn, pdir)
			if len(mode)>0: self.node.st.st_mode = mode[0]

		filemode = self.node.st.st_mode
		if (filemode & stat.S_IFREG) == 0:
			print "resulting file is no regular file, not allowed!"
			return -errno.EACCES
	

	def __writechunk(self):
		"writes active chunk back into store"
		if not self.actcmodified: return
		assert self.actchunk != None, self.actcidx>=0

		hash = chunkstore.put(self.actchunk)
		self.node.hashes[self.actcidx] = hash
		self.actcmodified = False


	def __loadchunk(self, chunkindex):
		"loads a new active chunk into the class, flushes the old one, if necessary"
		assert chunkindex>=0
		if (self.actcidx==chunkindex): return
		self.__writechunk()

		if chunkindex>=len(self.node.hashes):
			# fill up all non-existing space in file with empty chunks ...
			# create more than one chunk at once
			chunk = copy.copy(nullchunk)
			hash = chunkstore.put(chunk)
			for i in range(0, chunkindex-len(self.node.hashes)+1):
				self.node.hashes.append(hash)
			self.actchunk = chunk
		else:
			self.actchunk = chunkstore.get(self.node.hashes[chunkindex])

		self.actcidx = chunkindex
		self.actcmodified = False


	def read(self, length, offset):
		print "CBFSFilehandle.read(%s, %s)" % (length, offset)
		chunkidx = int(offset/chunkstore.chunksize)
		chunkamount = int(length/chunkstore.chunksize)+1
		byteidx = offset-chunkidx*chunkstore.chunksize
		bytesleft = min(length, self.node.st.st_size-offset)
		data = ""

		for chunkid in range(chunkidx, chunkidx+chunkamount):
			self.__loadchunk(chunkid)
			data += self.actchunk[byteidx:byteidx+bytesleft].tostring()
			bytesleft = length-len(data)
			byteidx = 0

		return data
	

	def write(self, buf, offset):
		print "CBFSFilehandle.write(data len: %s, %s)" % (len(buf), offset)
		bidx = 0

		while bidx<len(buf):
			cidx = int((offset+bidx)/chunkstore.chunksize)
			self.__loadchunk(cidx)
			coff = bidx+offset-cidx*chunkstore.chunksize
			count = len(buf)-bidx
			if (count > chunkstore.chunksize-coff):
				# wrap length to chunk limits
				count = chunkstore.chunksize-coff

			# write data to chunk
			if coff+count>len(self.actchunk): self.actchunk.extend('\0' * coff+count-len(self.actchunk))
			self.actchunk[coff:coff+count] = array('c', buf[bidx:bidx+count])
			self.actcmodified = True
			bidx += count
		
		if (len(buf)+offset>self.node.st.st_size):
			self.node.st.st_size = len(buf)+offset

		return len(buf)
	

	def release(self, flags):
		print "CBFSFilehandle.release()"
		self.__writechunk()
	

	def flush(self):
		print "CBFSFilehandle.flush()"
		self.__writechunk()


	def getattr(self):
		print "CBFSFilehandle.getattr()"
		return self.node.st


	def setattr(self, st):
		print "CBFSFilehandle.setattr(%s)" % st
		self.node.st = st


	def truncate(self, len):
		print "CBFSFilehandle.truncate(%s)" % len
		self.__writechunk()
		cno = int(len/chunkstore.chunksize)+1
		if (len<self.node.st.st_size):
			del(self.node.hashes[cno:])
		elif (len>self.node.st.st_size):
			self.__loadchunk(cno)
		self.node.st.st_size = len


	# def fsync(self, isfsyncfile):	
	# def lock(self, cmd, owner, **kw):

	


class CBFS(Fuse):

	workdir = None
	dirtree = None

	def __init__(self, *args, **kw):
		if not ('unittest' in kw and kw['unittest']):
			Fuse.__init__(self, *args, **kw)
		self.dirtree = CBFSDirtree()

		# passes complete dirtree to other classes (e.g. CBFSFilehandle)
		global dirtree
		dirtree = self.dirtree


	def getattr(self, path):
		print "getattr(%s)" % path
		try:
			print "returning path:"
			s = self.dirtree.getnode(path).st
			s.printme()
			return s
		except:
			print "getattr: node not found"
			traceback.print_exc()
			return -errno.ENOENT


	def readdir(self, path, offset):
		print "readdir(%s, %s)" % (path, offset)
		try:
			dir = self.dirtree.getnode(path)
		except:
			print "dir not found"
			traceback.print_exc()
			yield -errno.ENOENT
			return
		if not isinstance(dir, CBFSDirectory):
			print "dir not a directory"
			yield -errno.ENOENT
			return
		yield fuse.Direntry(".")
		yield fuse.Direntry("..")
		for e in dir.entries:
			print "readdir: yielding %s" % dir.entries[e].name
			yield dir.entries[e]


	def readlink(self, path):
		print "readlink(%s)" % path
		try:
			dir = self.dirtree.getnode(path)
		except:
			traceback.print_exc()
			return -errno.ENOENT

		if not isinstance(dir, CBFSSymlink):
			return -errno.EACCES
		return dir.dest
			
		
	def unlink(self, path):
		print "unlink(%s)" % path
		try:
			dir = self.dirtree.getnode(path)
			if isinstance(dir, CBFSDirectory):
				return -errno.EACCES
			parent = dir.parent
			del(parent.entries[dir.name])
		except:
			traceback.print_exc()
			return -errno.ENOENT


	def rmdir(self, path):
		print "rmdir(%s)" % path
		try:
			dir = self.dirtree.getnode(path)
		except:
			traceback.print_exc()
			return -errno.ENOENT
		if (not isinstance(dir, CBFSDirectory)) or len(dir.entries)>0:
			return -errno.EACCES
		parent = dir.parent
		print "parent: %s" % parent.name
		del(parent.entries[dir.name])


	def symlink(self, path, path1):
		print "symlink(%s, %s)" % (path, path1)
		(dirname, sep, symname) = path.rpartition("/")
		try:
			dir = self.dirtree.getnode(dirname)
			if not isinstance(dir, CBFSDirectory):
				return -errno.EACCES
			dir.entries[symname] = CBFSSymlink(symname, dir, path1)
		except:
			traceback.print_exc()
			return -errno.ENOENT
		

	def rename(self, path, path1):
		print "rename(%s, %s)" % (path, path1)
		(dirname1, sep, name1) = path.rpartition("/")
		(dirname2, sep, name2) = path1.rpartition("/")
		try:
			dir1 = self.dirtree.getnode(dirname1)
			dir2 = self.dirtree.getnode(dirname2)
			entry1 = dir1.entries[name1]
		except:
			traceback.print_exc()
			return -errno.ENOENT

		if name2 in dir2.entries:
			return -errno.EEXIST

		dir2.entries[name2] = entry1
		entry1.name = name2;
		del(dir1.entries[name1])


	#def link(self, path, path1):


	def chmod(self, path, mode):
		print "chmod(%s, %s)" % (path, mode)
		try:
			dir = self.dirtree.getnode(path)
		except:
			traceback.print_exc()
			return -errno.ENOENT
		dir.st.st_mode = mode


	def chown(self, path, user, group):
		print "chown(%s, %s, %s)" % (path, user, group)
		try:
			dir = self.dirtree.getnode(path)
		except:
			traceback.print_exc()
			return -errno.ENOENT
		if user>=0: dir.st.st_uid = user
		if group>=0: dir.st.st_gid = group


	def truncate(self, path, len):
		print "truncate(%s, %s)" % (path, len)
		fh = CBFSFilehandle(path, None)
		fh.truncate(len)


	def mknod(self, path, mode, dev):
		print "mknod(%s, %s, %s)" % (path, mode, dev)
		if not mode&stat.S_IFREG:
			return -errno.EOPNOTSUPP
		(dirname, sep, newfilename) = path.rpartition("/")
		try:
			dir = self.dirtree.getnode(dirname)
		except:
			traceback.print_exc()
			return -errno.ENOENT
		dir.entries[newfilename] = CBFSFile(newfilename, dir)


	def mkdir(self, path, mode):
		print "mkdir(%s, %s)" % (path, mode)
		(dirname, sep, newdirname) = path.rpartition("/")
		try:
			dir = self.dirtree.getnode(dirname)
		except:
			traceback.print_exc()
			return -errno.ENOENT
		if newdirname in dir.entries:
			return -errno.EEXIST
		dir.entries[newdirname] = CBFSDirectory(newdirname, dir)
		

	def utime(self, path, times):
		print "utime(%s, %s)" % (path, times)
		try:
			st = self.dirtree.getnode(path).st
			st.st_atime = times[0]
			st.st_mtime = times[1]
		except:
			print "utime: node not found (%s)!" % sys.exc_info()[0]
			traceback.print_exc()
			return -errno.ENOENT
		

	def access(self, path, mode):
		print "access(%s, %s): ******** not implemented!" % (path, mode)
		return

	
	def setattr(self, path, st):
		print "setattr(%s, %s): ******** not implemented!" % (path, st)
		print


	def statfs(self):
		print "statfs()"
		stat = fuse.StatVFS()
		stat.f_bsize = chunkstore.chunksize
		stat.f_frsize = stat.f_bsize
		stat.f_blocks = 111
		stat.f_bfree = 222
		stat.f_files = 333
		stat.f_ffree = 444

		return stat


	def fsinit(self):
		print "fsinit()"
		self.workdir = "tmp"
		global chunkstore
		chunkstore = ChunkStore(self.workdir, 2**19)
		global nullchunk
		nullchunk = array('c', '\0' * chunkstore.chunksize)
		try:
			hash = chunkstore.loadinithash()
			print "loading dirtree from hash %s" % hash
			self.dirtree.load(hash)
		except Exception, e:
			traceback.print_exc()
			print "exception: %s" % e
	

	def fsdestroy(self):
		print "fsdestroy()"
		hash = self.dirtree.save()
		print "dirtree hash: %s" % hash
		chunkstore.saveinithash(hash)
	

	def main(self, *a, **kw):
		self.file_class = CBFSFilehandle
		return Fuse.main(self, *a, **kw)


def main():
    usage="""
Chunk-based filesystem

""" + Fuse.fusage
    server = CBFS(version="%prog " + fuse.__version__, usage=usage, dash_s_do='setsingle')

    server.parse(errex=1)
    server.main()

if __name__ == '__main__':
    main()
