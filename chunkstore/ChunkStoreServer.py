from __future__ import with_statement

import os
import fnmatch
import hashlib
import sys
import ConfigParser
import BaseHTTPServer
import SocketServer
from array import array 

import CSProtServer


class ChunkStoreServer:
    "implements a local chunk store that is accessed by ChunkStoreManager"

    rootdir = "."
    hashalgo = hashlib.sha512()
    hashsize = 128
    maxspace = None
    curspace = None

    def __init__(self, rootdir, maxspace):
        print "ChunkStoreServer.__init__(%s, %s)" % (rootdir, maxspace)
        self.maxspace = maxspace
        self.rootdir = rootdir
        self.thread_stopped = False
        if not os.path.exists(self.rootdir): 
            os.makedirs(self.rootdir)
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

        hash = self.hashalgo.copy()
        hash.update(chunk)
        d = hash.hexdigest()
        print "ChunkStore.put: hash '%s'" % d
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
        return d


    def get(self, hashstring):
        # special case for zero-length chunks
        if hashstring=="0":
            return array('c')

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
    

    def saveinithash(self, hash):
        with open("%s/inithash" % self.rootdir, "w") as f:
            f.truncate(0)
            f.write(hash)


    def loadinithash(self):
        with open("%s/inithash" % self.rootdir, "r") as f:
            return f.readline()


    def remove(self, hashstring):
        (fpath, fname) = self.__calcfilename(hashstring)
        compname = "%s/%s/%s" % (self.rootdir, fpath, fname)
        self.curspace -= os.stat(compname).st_size
        os.remove(compname)
        # TODO: remove paths which are unneeded

    
    def get_stored_hashes(self):
        files = []
        for p1 in os.listdir(self.rootdir):
            print "p1: %s" % p1
            if os.path.isdir("%s/%s" % (self.rootdir, p1)):
                for p2 in os.listdir("%s/%s" % (self.rootdir, p1)):
                    print "p2: %s" % p2
                    if os.path.isdir("%s/%s/%s" % (self.rootdir, p1, p2)):
                        for file in os.listdir("%s/%s/%s" % (self.rootdir, p1, p2)):
                            print "file: %s" % file
                            if fnmatch.fnmatch(file, "*.dat"):
                                files.append(''.join([p1, p2, file[:-4]]))
        print "get_stored_hashes: returning list length %d" % len(files)
        return files


    def _updateFreeSpace(self):
        if self.curspace is None:
            self.curspace = 0
            for file in os.listdir(self.rootdir):
                if fnmatch.fnmatch(file, '*.dat'):
                    self.curspace += os.stat("%s/%s" % (self.rootdir, file)).st_size
        print "ChunkStoreServer.updateFreeSpace(): %d bytes used" % self.curspace

    
    def get_free_space(self):
        self._updateFreeSpace()
        print "ChunkStoreServer.get_free_space: %d" % (self.maxspace-self.curspace)
        return self.maxspace-self.curspace

    def get_used_space(self):
        self._updateFreeSpace()
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
    dispatcher = SocketServer.ForkingTCPServer(('', port), CSProtServer.CSProtServer)
    print 'listening on %d ...' % port
    
    try:
        dispatcher.serve_forever()
    except KeyboardInterrupt:
        pass
    dispatcher.server_close()
