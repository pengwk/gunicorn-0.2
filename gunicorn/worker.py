# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.


import errno
import logging
import os
import select
import signal
import socket
import sys
import tempfile

from gunicorn import http
from gunicorn import util


class Worker(object):

    SIGNALS = map(
        lambda x: getattr(signal, "SIG%s" % x),
        "HUP QUIT INT TERM TTIN TTOU USR1".split()
    )

    def __init__(self, workerid, ppid, socket, app, timeout):
        self.id = workerid
        self.ppid = ppid

        # 为什么要除？？
        self.timeout = timeout / 2.0

        # 这个临时文件用来做什么？
        fd, tmpname = tempfile.mkstemp()
        self.tmp = os.fdopen(fd, "r+b")
        self.tmpname = tmpname
        
        # prevent inherientence
        # ？？
        self.socket = socket
        util.close_on_exec(self.socket)
        # 非阻塞
        self.socket.setblocking(0)
                
        util.close_on_exec(fd)

        self.address = self.socket.getsockname()
        
        self.app = app
        self.alive = True
        self.log = logging.getLogger(__name__)
    
        
    def init_signals(self):
        # signal.SIG_DFL 这是两种标准信号处理选项之一；它只会执行信号的默认函数。 例如，在大多数系统上，对于 SIGQUIT 的默认操作是转储核心并退出，而对于 SIGCHLD 的默认操作是简单地忽略它。
        map(lambda s: signal.signal(s, signal.SIG_DFL), self.SIGNALS)
        signal.signal(signal.SIGQUIT, self.handle_quit)
        signal.signal(signal.SIGTERM, self.handle_exit)
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGUSR1, self.handle_quit)
    
    def handle_quit(self, sig, frame):
        self.alive = False

    def handle_exit(self, sig, frame):
        sys.exit(0)
        
    def _fchmod(self, mode):
        # 兼容
        if getattr(os, 'fchmod', None):
            os.fchmod(self.tmp.fileno(), mode)
        else:
            os.chmod(self.tmpname, mode)
    
    def run(self):
        self.init_signals()
        spinner = 0 
        while self.alive:
            
            nr = 0
            # Accept until we hit EAGAIN. We're betting that when we're
            # processing clients that more clients are waiting. When
            # there's no more clients waiting we go back to the select()
            # loop and wait for some lovin.
            while self.alive:
                try:
                    client, addr = self.socket.accept() 
                    
                    # handle connection
                    self.handle(client, addr)

                    # Update the fd mtime on each client completion
                    # to signal that this worker process is alive.
                    spinner = (spinner+1) % 2
                    self._fchmod(spinner)
                    nr += 1
                except socket.error, e:
                    # ECONNABORTED Software caused connection abort
                    # EAGAIN try again
                    if e[0] in (errno.EAGAIN, errno.ECONNABORTED):
                        break # Uh oh!
                    
                    raise
                if nr == 0: break
                
            while self.alive:
                spinner = (spinner+1) % 2
                self._fchmod(spinner)
                try:
                    ret = select.select([self.socket], [], [], 
                                    self.timeout)
                    if ret[0]:
                        break
                except select.error, e:
                    # EINTR Interrupted system call
                    if e[0] == errno.EINTR:
                        break
                    raise
                    
            spinner = (spinner+1) % 2
            self._fchmod(spinner)
            

    def handle(self, client, addr):
        util.close_on_exec(client)
        try:
            req = http.HttpRequest(client, addr, self.address)
            # WSGI 协议
            response = self.app(req.read(), req.start_response)
            http.HttpResponse(client, response, req).send()
        except Exception, e:
            self.log.exception("Error processing request. [%s]" % str(e))    
        finally:    
            util.close(client)
            
