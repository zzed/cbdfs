from __future__ import with_statement

import os
import fnmatch
import hashlib
import sys
import ConfigParser
import BaseHTTPServer
import SocketServer
import traceback
from array import array 

import CSProtServer


class ChunkStoreServer:
    "implements a local chunk store that is accessed by ChunkStoreManager"

    rootdir = "."
    hashalgo = hashlib.sha512()
    hashsize = 128
    maxspace = None
    curspace = None
    _storedHashes = None

    def __init__(self, rootdir, maxspace):
        print "ChunkStoreServer.__init__(%s, %s)" % (rootdir, maxspace)
        self.maxspace = maxspace
        self.rootdir = rootdir
        self.thread_stopped = False
        if not os.path.exists(self.rootdir): 
            os.makedirs(self.rootdir)
        self._updateStoredHashes()
        self.get_free_space()
        


    def stop(self):
        print "ChunkStoreServer.stop()"
        #self.shutdown = True
        #while not self.thread_stopped:
            #time.sleep(1)

    def __calcfilename(self, hexdigest):
        "calculates path and filename of chunk file"
        return ("%s/%s" % (hexdigest[0:2], hexdigest[2:4]), "%s.dat" % hexdigest[4:])

    def put(self, chunk):
        print "ChunkStore.put(chunklen: %d)" % len(chunk)

        # special case for zero-length chunks
        if len(chunk)==0:
            return "0";

        try:
	        hash = self.hashalgo.copy()
	        hash.update(chunk)
	        d = hash.hexdigest()
	        print "ChunkStore.put: hash '%s'" % d
	        
	        if d in self._storedHashes:
	        	# already in filesystem stored, we do not need to do anything
	        	return d
	        
	        (fpath, fname) = self.__calcfilename(d)
	        comppath = "%s/%s" % (self.rootdir, fpath)
	        compname = "%s/%s" % (comppath, fname)
	        print "compname: " + compname
	        if not os.access(comppath, os.X_OK):
	            os.makedirs(comppath)
	        if os.access(compname, os.F_OK|os.R_OK|os.W_OK):
	            # file already exists, we don't need to do anything
	            print "ChunkStore.put: file already exists"
	            return d
	        with open(compname, "w") as f:
	            chunk.tofile(f)
	            self.curspace += len(chunk)
	        self._storedHashes.append(d)
	        return d
        except:
        	traceback.print_exc()
	    	return ""


    def get(self, hashstring):
        # special case for zero-length chunks
        if hashstring=="0":
            return array('c')

        try:
	        (fpath, fname) = self.__calcfilename(hashstring)
	        compname = "%s/%s/%s" % (self.rootdir, fpath, fname)
	        assert(os.access(compname, os.F_OK|os.R_OK), "failed to access file %s" % compname)
	        s = os.stat(compname).st_size
	        with open(compname, "r") as f:
	            chunk = array('c')
	            chunk.fromfile(f, s)
	        hash = self.hashalgo.copy()
	        hash.update(chunk)
	        d = hash.hexdigest()
	        assert(d==hashstring, "file %s does not have correct checksum %s" % (compname, hashstring))
	
	        print "returning chunk for hash %s" % hashstring
	        return chunk
        except:
        	traceback.print_exc()
	    	return array('c')
	   
    

    def saveinithash(self, hash):
        with open("%s/inithash" % self.rootdir, "w") as f:
            f.truncate(0)
            f.write(hash)


    def loadinithash(self):
    	try:
            with open("%s/inithash" % self.rootdir, "r") as f:
                return f.readline()
        except:
	    	print "ChunkStoreServer: failed to load inithash"
	    	return ""


    def remove(self, hashstring):
    	print "ChunkStoreServer.remove(%s)" % hashstring
    	try:
	        (fpath, fname) = self.__calcfilename(hashstring)
	        compname = "%s/%s/%s" % (self.rootdir, fpath, fname)
	        self.curspace -= os.stat(compname).st_size
	        os.remove(compname)	        
        except:
	    	traceback.print_exc()

        if hashstring in self._storedHashes:
        	self._storedHashes.remove(hashstring)

    
    def get_stored_hashes(self):
        return self._storedHashes


    def _updateStoredHashes(self):
    	print "Indexing currently stored hashes, please wait ..."
    	print "[" + (' ' * 16) + "]\r[",
        self.curspace = 0
        self._storedHashes = []
        count = 1
        for p1 in os.listdir(self.rootdir):
            count += 1
            if count%16==0:
                sys.stdout.write(".")
                sys.stdout.flush()
            if os.path.isdir("%s/%s" % (self.rootdir, p1)):
                for p2 in os.listdir("%s/%s" % (self.rootdir, p1)):
                    if os.path.isdir("%s/%s/%s" % (self.rootdir, p1, p2)):
                        for file in os.listdir("%s/%s/%s" % (self.rootdir, p1, p2)):
                            if fnmatch.fnmatch(file, "*.dat"):
                                self._storedHashes.append(''.join([p1, p2, file[:-4]]))
                                self.curspace += os.stat("%s/%s/%s/%s" % (self.rootdir, p1, p2, file)).st_size
        for i in range(count/16,16):
        	sys.stdout.write(".")
        print "\nStored hashes: %d" % len(self._storedHashes)
        print "Used space: %d bytes" % self.curspace

    
    def get_free_space(self):
        print "ChunkStoreServer.get_free_space: %d" % (self.maxspace-self.curspace)
        return self.maxspace-self.curspace

    def get_used_space(self):
        print "ChunkStoreServer.get_used_space: %d" % (self.curspace)
        return self.curspace


def load_config(configfile):
    section = "ChunkStoreServer"
    cp = ConfigParser.ConfigParser()
    cp.read(configfile)
    rootdir = cp.get(section, "chunkdir")
    s = cp.get(section, "maxspace")
    s = s.replace("MB", "")
    maxspace = int(s)*1024*1024
    port = cp.getint(section, "port")
    host = cp.get(section, "hostname")
    return (rootdir, maxspace, port, host)


# main execution
if __name__ == '__main__':
    print "server starting ..."

    if len(sys.argv)!=2:
        print "usage: <command> <configfile>"
        sys.exit(1)

    (rootdir, maxspace, port, host) = load_config(sys.argv[1])

    csserver = ChunkStoreServer(rootdir, maxspace)
    CSProtServer.CSProtServer.csserver = csserver
    dispatcher = CSProtServer.CSProtServer('', port)
    #dispatcher = SocketServer.ForkingTCPServer(('', port), CSProtServer.CSProtServer)
    print 'listening on %d ...' % port
    
    try:
        dispatcher.serve_forever()
    except KeyboardInterrupt:
        pass
    dispatcher.server_close()
