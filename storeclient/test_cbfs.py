#!/usr/bin/env python

import cbfs, os, stat

from chunkstore import ChunkStore


def compdirs(direntries, names):
	'''compares two lists with direntries, if all elements contain the same name'''
	direntries = list(direntries)
	if len(direntries) != len(names): 
		n = []
		for i in direntries: n.append(i.name)
		print("compdirs: received: %s\n          expected: %s" % (n, names))
		return False
	for i in direntries:
		if not i.name in names:
			n = []
			for i in direntries: n.append(i.name)
			print("compdirs: received: %s\n          expected: %s" % (n, names))
			return False
	return True


# setup
fs = cbfs.CBFS(unittest=True)
fs.fsinit()

assert(compdirs(fs.readdir("/", 0), [ ".", ".." ]))

# test mkdir
assert(fs.mkdir("/testdir1", 0)==None)
assert(compdirs(fs.readdir("/", 0), [ ".", "..", "testdir1" ]))
assert(fs.getattr("/testdir1").st_mode&stat.S_IFDIR)
assert(fs.mkdir("/testdir2", 0)==None)
assert(compdirs(fs.readdir("/", 0), [ ".", "..", "testdir1", "testdir2" ]))

# save and load fs-stat
hash = fs.dirtree.save()
assert(fs.dirtree.load(hash)==None)
assert(compdirs(fs.readdir("/", 0), [ ".", "..", "testdir1", "testdir2" ]))

# test rmdir
assert(fs.rmdir("/testdir2")==None)
assert(compdirs(fs.readdir("/", 0), [ ".", "..", "testdir1" ]))
assert(fs.rmdir("/testdir1")==None)
assert(compdirs(fs.readdir("/", 0), [ ".", ".." ]))

# file reading and writing
f = cbfs.CBFSFilehandle("/testfile", os.O_CREAT|os.O_WRONLY, stat.S_IFREG)
f.write("test123", 0)
f.release(0)
assert(compdirs(fs.readdir("/", 0), [ ".", "..", "testfile" ]))
assert(fs.getattr("/testfile").st_mode&stat.S_IFREG)
assert(fs.getattr("/testfile").st_size == 7)
f = cbfs.CBFSFilehandle("/testfile", os.O_CREAT|os.O_RDONLY, 0)
assert(f.read(20, 0) == "test123")
f = cbfs.CBFSFilehandle("/testfile", os.O_CREAT|os.O_RDONLY, 0)
assert(f.read(20, 3) == "t123")
# TODO: big file

assert(fs.unlink("/testfile")==None)
assert(compdirs(fs.readdir("/", 0), [ ".", ".." ]))

print "\nTests successfully completed! (yai!!)"
