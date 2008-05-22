import SocketServer
import pdb
from array import array

class CSProtServer(SocketServer.StreamRequestHandler):
    "manages communication with ChunkStoreManager by offering a HTTP server"    

    csserver = None  # set by main method

    def handle(self):
        try:
            print "CSProtServer.handle: got connection from %s" % str(self.client_address)
            while (True):
                cmd = self.rfile.readline();
                cmd = cmd.rstrip()
                print "cmd: '%s'" % cmd
    
                if cmd=="get_stored_hashes":
                    h = self.csserver.get_stored_hashes()
                    self.wfile.write("%s\n" % len(h))
                    for i in h:
                        self.wfile.write(i + "\n")
                elif cmd=="get_free_space":
                    self.wfile.write(str(self.csserver.get_free_space()) + "\n")
                elif cmd=="get_used_space":
                    self.wfile.write(str(self.csserver.get_used_space()) + "\n")
                elif cmd=="put":
                    size = self.rfile.readline().rstrip()
                    print "CSProtServer.do_GET: size: %s" % size
                    size = int(size)
                    cstr = self.rfile.read(size)
                    chunk = array('c', cstr)
                    hash = self.csserver.put(chunk)
                    self.wfile.write(hash + "\n")
                elif cmd=="get":
                    hash = self.rfile.readline().rstrip()
                    chunk = self.csserver.get(hash)
                    self.wfile.write("%d\n" % len(chunk))
                    self.wfile.write(chunk.tostring())
                elif cmd=="remove":
                    hash = self.rfile.readline().rstrip()
                    self.csserver.remove(hash)
                elif cmd=="save_init_hash":
                    hash = self.rfile.readline().rstrip()
                    self.csserver.saveinithash(hash)
                elif cmd=="load_init_hash":
                    self.wfile.write(self.csserver.loadinithash() + "\n")
                self.wfile.flush()
        except:
            print "CSProtServer.handle: lost connection"
            raise
        finally: 
            self.rfile.close()
            self.wfile.close()
