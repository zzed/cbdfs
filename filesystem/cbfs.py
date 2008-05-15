#!/usr/bin/env python

from __future__ import with_statement
import os, sys, errno, copy, traceback
from ChunkStoreManager import ChunkStoreManager

sys.path.append("fuse-build")
import fuse
import traceback
import stat
from array import array
from fuse import Fuse

from CBFilesystem import CBFSStat, CBFSDirectory, CBFSFile, CBFSSymlink, CBFSDirtree


if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."

fuse.fuse_python_api = (0, 2)


			

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
			chunk = array('c')
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
		try:
			dirtree.lock.acquire()
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
		finally:
			dirtree.lock.release()
		return data
	

	def write(self, buf, offset):
		print "CBFSFilehandle.write(data len: %s, %s)" % (len(buf), offset)
		try:
			dirtree.lock.acquire()
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
				if coff+count>len(self.actchunk): self.actchunk.extend('\0' * (coff+count-len(self.actchunk)))
				self.actchunk[coff:coff+count] = array('c', buf[bidx:bidx+count])
				self.actcmodified = True
				bidx += count
			
			if (len(buf)+offset>self.node.st.st_size):
				self.node.st.st_size = len(buf)+offset
		finally:
			dirtree.lock.release()
		return len(buf)
	

	def release(self, flags):
		print "CBFSFilehandle.release()"
		try:
			dirtree.lock.acquire()
			self.__writechunk()
		finally:
			dirtree.lock.release()
	

	def flush(self):
		print "CBFSFilehandle.flush()"
		try:
			dirtree.lock.acquire()
			self.__writechunk()
		finally:
			dirtree.lock.release()


	def getattr(self):
		print "CBFSFilehandle.getattr()"
		return self.node.st


	def setattr(self, st):
		print "CBFSFilehandle.setattr(%s)" % st
		self.node.st = st


	def truncate(self, len):
		print "CBFSFilehandle.truncate(%s)" % len
		try:
			dirtree.lock.acquire()
			self.__writechunk()
			cno = int(len/chunkstore.chunksize)+1
			if (len<self.node.st.st_size):
				del(self.node.hashes[cno:])
			elif (len>self.node.st.st_size):
				self.__loadchunk(cno)
			self.node.st.st_size = len
		finally:
			dirtree.lock.release()

	# def fsync(self, isfsyncfile):	
	# def lock(self, cmd, owner, **kw):
	


class CBFS(Fuse):

	dirtree = None
	chunkstore = None

	def __init__(self, *args, **kw):
		self.dirtree = CBFSDirtree()
		if not ('unittest' in kw and kw['unittest']):
			Fuse.__init__(self, *args, **kw)
			#print "args: " + str(sys.argv)
			#if '-c' in sys.argv:
				#i = sys.argv.index('-c')
				#if i>=len(sys.argv)-1:
					#raise Exception("Configuration file not specified!")
				#config = sys.argv[i+1]
			#else:
				#raise Exception("Configuration file not specified!")
			config = "cbfs.conf"
			self.chunkstore = ChunkStoreManager(self.dirtree, config)

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


	def symlink(self, target, name):
		print "symlink(%s, %s)" % (target, name)
		(dirname, sep, symname) = name.rpartition("/")
		try:
			dir = self.dirtree.getnode(dirname)
			if not isinstance(dir, CBFSDirectory):
				return -errno.EACCES
			dir.entries[symname] = CBFSSymlink(symname, dir, target)
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
		csstat = self.chunkstore.getSpaceStat()
		stat = fuse.StatVfs()
		stat.f_bsize = 1
		stat.f_frsize = stat.f_bsize
		stat.f_blocks = csstat[1]
		stat.f_bfree = csstat[1]-csstat[0]
		stat.f_bavail = stat.f_bfree
		stat.f_files = csstat[0]
		stat.f_ffree = stat.f_bfree
		stat.f_favail = stat.f_bfree

		return stat


	def fsinit(self):
		print "fsinit()"
		global chunkstore
		chunkstore = self.chunkstore
		self.dirtree.chunkstore = self.chunkstore
		try:
			self.dirtree.load()
		except Exception, e:
			traceback.print_exc()
	

	def fsdestroy(self):
		print "fsdestroy()"
		hash = self.dirtree.save()
		print "dirtree hash: %s" % hash
		self.chunkstore.stop()
	

	def main(self, *a, **kw):
		self.file_class = CBFSFilehandle
		return Fuse.main(self, *a, **kw)


def main():
    usage="""
Chunk-based filesystem

specify -c <config file>

""" + Fuse.fusage
    server = CBFS(version="%prog " + fuse.__version__, usage=usage, dash_s_do='setsingle')

    server.parse(errex=1)
    server.main()

if __name__ == '__main__':
    main()
