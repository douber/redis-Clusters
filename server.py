import socket
import logging
from os.path import basename, join
import zookeeper
from collections import namedtuple  
import os
import sys
import threading
import signal
from time import ctime
import time
import json
import select, errno

# Mapping of connection state values to human strings.  
STATE_NAME_MAPPING = {  
    zookeeper.ASSOCIATING_STATE: "associating",  
    zookeeper.AUTH_FAILED_STATE: "auth-failed",  
    zookeeper.CONNECTED_STATE: "connected",  
    zookeeper.CONNECTING_STATE: "connecting",  
    zookeeper.EXPIRED_SESSION_STATE: "expired",  
}  
  
# Mapping of event type to human string.  
TYPE_NAME_MAPPING = {  
    zookeeper.NOTWATCHING_EVENT: "not-watching",  
    zookeeper.SESSION_EVENT: "session",  
    zookeeper.CREATED_EVENT: "created",  
    zookeeper.DELETED_EVENT: "deleted",  
    zookeeper.CHANGED_EVENT: "changed",  
    zookeeper.CHILD_EVENT: "child",   
}

DEFAULT_TIMEOUT = 30000

logging.basicConfig(level = logging.DEBUG,format = "[%(asctime)s] %(levelname)-8s %(message)s")
log = logging
logger = logging.getLogger("network-server")

class ClientEvent(namedtuple("ClientEvent", 'type, connection_state, path')):  
    """ 
    A client event is returned when a watch deferred fires. It denotes 
    some event on the zookeeper client that the watch was requested on. 
    """  
 
    @property  
    def type_name(self):  
        return TYPE_NAME_MAPPING[self.type]  
 
    @property  
    def state_name(self):  
        return STATE_NAME_MAPPING[self.connection_state]  
  
    def __repr__(self):  
        return  "<ClientEvent %s at %r state: %s>" % (  
            self.type_name, self.path, self.state_name)  

def watchmethod(func):  
    def decorated(handle, atype, state, path):  
        event = ClientEvent(atype, state, path)  
        return func(event)  
    return decorated  


class MYServer(object):

	def __init__(self,timeout=DEFAULT_TIMEOUT):
		self.conn_cv = threading.Condition()
		self.conn_cv.acquire() 
		self.connected = False
		self.zk = -1
		self.children = []
		logger.debug("Connecting to %s" % ('127.0.0.1:2181'))
		start = time.time()  
		self.zk = zookeeper.init("127.0.0.1:2181",self.connection_watcher,timeout)
		self.conn_cv.wait(timeout/1000)
		self.conn_cv.release()
		if not self.connected:
			self.say("Unable to connect to %s" % ('127.0.0.1:2181'))
		logger.debug("Connected in %d ms, handle is %d" % (int((time.time() - start) * 1000), self.zk)) 
		self.master()

	def connection_watcher(self, h, type, state, path):
		self.zk = h
		self.conn_cv.acquire()
		self.connected = True
		self.conn_cv.notifyAll()
		self.conn_cv.release()  

	def master(self):
		"""
		get children, and check who is the smallest child
		"""
		@watchmethod
		def watcher(event):
			self.say("child changed, try to get master again.")
			self.master()
		self.children = zookeeper.get_children(self.zk,'/redis_master', watcher)
		self.children.sort()
		logger.debug("%s's children: %s" % ('/redis_master', self.children)) 
	
	def say(self, msg):
		"""
		print messages to screen
		"""
		log.info(msg)

class Watcher:
	def __init__(self):
		""" Creates a child thread, which returns.  The parent
			thread waits for a KeyboardInterrupt and then kills
			the child thread.
		"""
		self.child = os.fork()
		if self.child == 0:
			return
		else:
			self.watch()

	def watch(self):
		try:
			os.wait()
		except KeyboardInterrupt:
			print ' exit...'
			self.kill()
		sys.exit()

	def kill(self):
		try:
			os.kill(self.child, signal.SIGKILL)
		except OSError:
			pass

def start_zk_worker():

	zkserver = MYServer()
	server = threading.Thread(target = TCPserver, name = "server", args = (zkserver,))
	#option = threading.Thread(target = TCPoption, name = "option", args = (zkserver,))
	server.start()

def TCPserver(zkserver):
	connections = {};addresses = {};datalist = {}
	try:
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM,0)
	except socket.error, msg:
		logger.error("create a socket failed")
	try:
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	except socket.error,msg:
		logger.error("setsocketopt error")
	try:
		sock.bind(('', 8800))
	except socket.error,msg:
		logger.error("listen file id bind ip error")
	try:
		sock.listen(2000)
	except socket.error,msg:
		logger.error(msg)

	logger.debug("server runing listen on port 8800 ...")
	try:
		epoll = select.epoll()
		epoll.register(sock.fileno(), select.EPOLLIN)
	except socket.error,msg:
		logger.error(msg)
	
	while True:
		epoll_list = epoll.poll()
		for fd, events in epoll_list:
			if fd == sock.fileno():
				ss, addr = sock.accept()
				#logger.debug("accept connection from %s, %d, fd = %d" % (addr[0], addr[1], ss.fileno()))
				ss.setblocking(0)
				epoll.register(ss.fileno(), select.EPOLLIN | select.EPOLLET)
				connections[ss.fileno()] = ss
				addresses[ss.fileno()] = addr
			elif select.EPOLLIN & events:
				datas = ""
				while True:
					try :
						data = connections[fd].recv(1024)
						if not data and not datas:
							epoll.unregister(fd)
							connections[fd].close()
							#logger.debug("%s, %d closed" % (addresses[fd][0], addresses[fd][1]))
							break
						else:
							datas += data
					except socket.error, msg:
						if msg.errno == errno.EAGAIN:
							#logger.debug("%s receive %s" % (fd, datas))
							#datalist[fd] = datas
							epoll.modify(fd, select.EPOLLET | select.EPOLLOUT)
							break
						else:
							epoll.unregister(fd)
							connections[fd].close()
							log.info(msg)
							break
			elif select.EPOLLHUP & events:
				epoll.unregister(fd)
				connections[fd].close()
				#logger.debug("%s, %d closed" % (addresses[fd][0], addresses[fd][1])) 
			elif select.EPOLLOUT & events:
				connections[fd].send(json.dumps(zkserver.children)+"\r\n")
				epoll.modify(fd, select.EPOLLIN | select.EPOLLET)
			else:
				continue
	sock.close()

	
def InitLog():
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("log/network-server.log")
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)

def main():
	InitLog()
	Watcher()
	start_zk_worker()

if __name__ == "__main__":
	main()