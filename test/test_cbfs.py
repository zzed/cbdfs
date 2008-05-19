#!/usr/bin/env python

import os, stat, sys, thread, time, BaseHTTPServer, cProfile

# try to find paths to cbdfs modules

d = os.path.dirname(sys.argv[0])
sys.path.insert(0, d + '/../chunkstore')
sys.path.insert(0, d + '/../filesystem')

import cbfs
import CSProtClient
import CSProtServer
import ChunkStoreServer
import ChunkStoreManager


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

def clean_workdir(dir):
	try:
		for root, dirs, files in os.walk(dir, False):
			for f in files:
				os.unlink(root + "/" + f)
			os.rmdir(root)
	except:
		pass

def server_thread(workdir):
	css = ChunkStoreServer.ChunkStoreServer(workdir, 2**20)
	CSProtServer.CSProtServer.csserver = css
	srv = BaseHTTPServer.HTTPServer(("localhost", 9531), CSProtServer.CSProtServer )
	try:
		srv.serve_forever()
	except KeyboardInterrupt:
		pass
	srv.server_close()
	

def test_print(txt):
	print "=================== TEST ====================="
	print " %s" % (txt)
	print "=============================================="

def writeBigFile():
	buf = ""
	for i in range(0, 2000):
		buf += str(i)
	f = cbfs.CBFSFilehandle("/bigfile", os.O_CREAT|os.O_WRONLY, stat.S_IFREG)
	for i in range(0, 10*1024*1024/len(buf)):
		f.write(buf, i*len(buf))
	f.release(0)
	assert(fs.unlink("/bigfile")==None)


#=====================================
# prepare tests by starting client and server
workdir = "unittest_work"
clean_workdir(workdir)
os.mkdir(workdir)

thread.start_new_thread(server_thread, (workdir,))
time.sleep(1) # wait until server runs



#=====================================
# setup
fs = cbfs.CBFS(unittest=True)

csm = ChunkStoreManager.ChunkStoreManager(fs.dirtree, None, 512*1024)
cs = ChunkStoreManager.ChunkStore("localhost", 9531)

csm.chunkstores = [cs]
fs.dirtree.chunkstore = csm
fs.chunkstore = csm
fs.fsinit()


test_print("read /")
assert(compdirs(fs.readdir("/", 0), [ ".", ".." ]))

test_print("empty fs size")
sfs = fs.statfs()
assert(sfs.f_bsize==1)
assert(sfs.f_frsize==1)
assert(sfs.f_blocks==2**20)
assert(sfs.f_bfree==2**20)
assert(sfs.f_files==sfs.f_blocks-sfs.f_bfree)
assert(sfs.f_ffree==sfs.f_bfree)


# test mkdir
test_print("mkdir")
assert(fs.mkdir("/testdir1", 0)==None)
assert(compdirs(fs.readdir("/", 0), [ ".", "..", "testdir1" ]))
assert(fs.getattr("/testdir1").st_mode&stat.S_IFDIR)
assert(fs.mkdir("/testdir2", 0)==None)
assert(compdirs(fs.readdir("/", 0), [ ".", "..", "testdir1", "testdir2" ]))

# save and load fs-stat
test_print("test/load fs")
hash = fs.dirtree.save()
assert(fs.dirtree.load()==None)
assert(compdirs(fs.readdir("/", 0), [ ".", "..", "testdir1", "testdir2" ]))

# test rmdir
test_print("rmdir")
assert(fs.rmdir("/testdir2")==None)
assert(compdirs(fs.readdir("/", 0), [ ".", "..", "testdir1" ]))
assert(fs.rmdir("/testdir1")==None)
assert(compdirs(fs.readdir("/", 0), [ ".", ".." ]))

# file reading and writing
oldsfs = fs.statfs()
test_print("file read/write")
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
# test fs size
sfs = fs.statfs()
assert(sfs.f_bsize==1)
assert(sfs.f_frsize==1)
assert(sfs.f_blocks==2**20)
assert(sfs.f_bfree==oldsfs.f_bfree-7)
assert(sfs.f_files==sfs.f_blocks-sfs.f_bfree)
assert(sfs.f_ffree==sfs.f_bfree)

test_print("write big file: for profiling")
cProfile.run("writeBigFile()", "cbdfsprof")


# link reading and writing
test_print("link read/write")
assert(fs.symlink("testfile", "/link")==None)
assert(fs.readlink("/link")=="testfile")


test_print("remove")
assert(fs.unlink("/link")==None)
assert(fs.unlink("/testfile")==None)
assert(compdirs(fs.readdir("/", 0), [ ".", ".." ]))

# test hashes in store manager (DANGER: destroys inithash!)
test_print("hash enumeration")
h = cs.loadInitHash()
hashes = cs._client.get_stored_hashes()
assert(h in hashes)
cs.remove(h)
hashes = cs._client.get_stored_hashes()
assert(not h in hashes)


# TODO: test garbage collection!


fs.fsdestroy()
clean_workdir(workdir)

print "\nTests successfully completed! (yai!!)"
