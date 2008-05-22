import socket
import pdb
import traceback
from array import array

class CSProtServer:
    "manages communication with ChunkStoreManager by offering a HTTP server"    

    csserver = None  # set by main method
    s = None
    
    def __init__(self, host, port):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind((host, port))
        self.s.listen(1)
        
        
    def serve_forever(self):
        while True:
            conn, addr = self.s.accept()
            print 'Connected by', addr
            try:
                rfile = conn.makefile('rb', -1)
                wfile = conn.makefile('wb', 0)
                self.handle(rfile, wfile)
            except:
                traceback.print_exc()
            finally:
                rfile.close()
                wfile.close()

    def handle(self, rfile, wfile):
        try:
            while (True):
                cmd = rfile.readline()
                cmd = cmd.rstrip()
                print "cmd: '%s'" % cmd
                if cmd=="":
                    return
    
                if cmd=="get_stored_hashes":
                    h = self.csserver.get_stored_hashes()
                    wfile.write("%s\n" % len(h))
                    for i in h:
                        wfile.write(i + "\n")
                elif cmd=="get_free_space":
                    wfile.write(str(self.csserver.get_free_space()) + "\n")
                elif cmd=="get_used_space":
                    wfile.write(str(self.csserver.get_used_space()) + "\n")
                elif cmd=="put":
                    size = rfile.readline().rstrip()
                    print "CSProtServer.do_GET: size: %s" % size
                    size = int(size)
                    cstr = rfile.read(size)
                    chunk = array('c', cstr)
                    hash = self.csserver.put(chunk)
                    wfile.write(hash + "\n")
                elif cmd=="get":
                    hash = rfile.readline().rstrip()
                    chunk = self.csserver.get(hash)
                    wfile.write("%d\n" % len(chunk))
                    wfile.write(chunk.tostring())
                elif cmd=="remove":
                    hash = rfile.readline().rstrip()
                    self.csserver.remove(hash)
                elif cmd=="save_init_hash":
                    hash = rfile.readline().rstrip()
                    self.csserver.saveinithash(hash)
                elif cmd=="load_init_hash":
                    wfile.write(self.csserver.loadinithash() + "\n")
                wfile.flush()
                if wfile.closed or rfile.closed:
                    return
        except:
            print "CSProtServer.handle: lost connection"
            raise
