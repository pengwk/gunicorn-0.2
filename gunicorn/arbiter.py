# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.

import errno
import fcntl
import logging
import os
import select
import signal
import socket
import sys
import time

from gunicorn.worker import Worker

class Arbiter(object):
    
    # 为什么是类变量？

    # 一个 socket
    LISTENER = None
    # worker 信息，数据结构组成：pid: Worker instance
    WORKERS = {} 
    # 0: read 1: write
    # 管道用来做什么？
    PIPE = []

    # I love dyanmic languages
    # 信号队列？？不是没处理就抛弃了吗？
    # 收到之后自己维护在这里
    SIG_QUEUE = []
    # HUP 终端线挂断 默认行为程序终止
    # QUIT 来自键盘的退出 终止
    # INT 来自键盘的中断 终止 Ctrl + C  和上一个有什么区别？
    # TERM 软件终止信号，？？
    # TTOU 后台进程向终端写 停止直到下一个 SIGCOUNT
    # TTIN 后台进程向终端读
    # WINCH 窗口大小变化？？
    # USR1 USR2 用户定义信号 1，2
    # [signal.SIGTTIN, ...]
    SIGNALS = map(
        lambda x: getattr(signal, "SIG%s" % x),
        "HUP QUIT INT TERM TTIN TTOU USR1 USR2 WINCH".split()
    )
    # signal.SIGTTIN: 'ttin'
    SIG_NAMES = dict(
        (getattr(signal, name), name[3:].lower()) for name in dir(signal)
        if name[:3] == "SIG" and name[3] != "_"
    )
    
    def __init__(self, address, num_workers, modname):
        self.address = address
        self.num_workers = num_workers
        self.modname = modname
        self.timeout = 30
        self.reexec_pid = 0
        self.pid = os.getpid()
        self.log = logging.getLogger(__name__)
        self.init_signals()
        self.listen(self.address)
        self.log.info("Booted Arbiter: %s" % os.getpid())
        
                    
    def init_signals(self):
        if self.PIPE:
            map(lambda p: p.close(), self.PIPE)
        self.PIPE = pair = os.pipe()
        map(self.set_non_blocking, pair)
        # 设置文件描述符标记 
        # It marks the file descriptor so that it will be close()d automatically 
        # when the process or any children it fork()s calls one of the exec*() family of functions. 
        # This is useful to keep from leaking your file descriptors to random programs run by e.g. system().
        map(lambda p: fcntl.fcntl(p, fcntl.F_SETFD, fcntl.FD_CLOEXEC), pair)

        # 这些信号由 self.signal 统一处理
        map(lambda s: signal.signal(s, self.signal), self.SIGNALS)
        # 一个子进程停止或终止：worker 挂了
        signal.signal(signal.SIGCHLD, self.handle_chld)
    
    def set_non_blocking(self, fd):
        # Return (as the function result) the file access mode and the
        #      file status flags; arg is ignored.
        # 先读取已有的 flag 加上不阻塞的 flag 后设置回去
        flags = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
        fcntl.fcntl(fd, fcntl.F_SETFL, flags)

    def signal(self, sig, frame):
        if len(self.SIG_QUEUE) < 5:
            self.SIG_QUEUE.append(sig)
            self.wakeup()
        else:
            self.log.warn("Ignoring rapid signaling: %s" % sig)
        

    def listen(self, addr):
        if 'GUNICORN_FD' in os.environ:
            fd = int(os.environ['GUNICORN_FD'])
            del os.environ['GUNICORN_FD']
            try:
                sock = self.init_socket_fromfd(fd, addr)
                self.LISTENER = sock
                return
            except socket.error, e:
                # Transport endpoint is not connected
                if e[0] == errno.ENOTCONN:
                    self.log.error("should be a non GUNICORN environnement")
                else:
                    raise
        
        for i in range(5):
            try:
                sock = self.init_socket(addr)
                self.LISTENER = sock
                break
            except socket.error, e:
                # Address already in use
                if e[0] == errno.EADDRINUSE:
                    self.log.error("Connection in use: %s" % str(addr))
                if i < 5:
                    self.log.error("Retrying in 1 second.")
                time.sleep(1)
                
    def init_socket_fromfd(self, fd, address):
        sock = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)
        self.set_sockopts(sock)
        return sock

    def init_socket(self, address):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_sockopts(sock)
        sock.bind(address)
        # backlog 队列长度设置为 2048
        sock.listen(2048)
        return sock
        
    def set_sockopts(self, sock):
        # 非阻塞
        sock.setblocking(0)
        # https://lwn.net/Articles/542629/
        # 惊群效应和这里有什么关系？
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # https://stackoverflow.com/questions/3761276/when-should-i-use-tcp-nodelay-and-when-tcp-cork
        # TCP_NODELAY is used for disabling Nagle's algorithm.
        # 有数据直接发
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # TCP_CORK (or TCP_NOPUSH in FreeBSD)
        # TCP_CORK (since Linux 2.2)
        # If set, don't send out partial frames. 
        # All queued partial frames are sent when the option is cleared again. 
        # This is useful for prepending headers before calling sendfile(2), 
        # or for throughput optimization. As currently implemented, 
        # there is a 200 millisecond ceiling on the time for which output is corked by TCP_CORK. 
        # If this ceiling is reached, then queued data is automatically transmitted. 
        # This option can be combined with TCP_NODELAY only since Linux 2.5.71. 
        # This option should not be used in code intended to be portable.
        # cork 塞住
        if hasattr(socket, "TCP_CORK"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
        elif hasattr(socket, "TCP_NOPUSH"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NOPUSH, 1)

    def run(self):
        # 
        self.manage_workers()
        while True:
            try:
                self.reap_workers()
                sig = self.SIG_QUEUE.pop(0) if len(self.SIG_QUEUE) else None
                if sig is None:
                    # 为什么要睡？？性能。有信号处理完立即做 wake up 然后马上就处理新的数据，不然就睡 1s。反正只是监控，管理。要是所有 worker 全部死了。。。。
                    self.sleep()
                    self.murder_workers()
                    self.manage_workers()
                    continue
                
                # 没有预定义的信号不处理
                if sig not in self.SIG_NAMES:
                    self.log.info("Ignoring unknown signal: %s" % sig)
                    continue
                
                signame = self.SIG_NAMES.get(sig)
                handler = getattr(self, "handle_%s" % signame, None)
                if not handler:
                    self.log.error("Unhandled signal: %s" % signame)
                    continue
                self.log.info("Handling signal: %s" % signame)
                handler()
                # 信号处理完继续
                self.wakeup()   
            except StopIteration:
                break
            except KeyboardInterrupt:
                self.stop(False)
                # 返回 -1 意味着什么？
                sys.exit(-1)
            except Exception:
                self.log.exception("Unhandled exception in main loop.")
                self.stop(False)
                sys.exit(-1)
                
        self.log.info("Master is shutting down.")
        self.stop()
        
    def handle_chld(self, sig, frame):
        # 唤醒自己
        self.wakeup()
        
    def handle_hup(self):
        self.log.info("Master hang up.")
        self.reexec()
        raise StopIteration
        
    def handle_quit(self):
        raise StopIteration
    
    def handle_int(self):
        self.stop(False)
        raise StopIteration
    
    def handle_term(self):
        self.stop(False)
        raise StopIteration

    def handle_ttin(self):
        # 这里只加减数量，每次循环来处理这个东西。
        self.num_workers += 1
    
    def handle_ttou(self):
        if self.num_workers > 0:
            self.num_workers -= 1
            
    def handle_usr1(self):
        self.kill_workers(signal.SIGUSR1)
    
    def handle_usr2(self):
        self.reexec()
        
    def handle_winch(self):
        # 干什么？？ 后台进程或者 父进程组不是当前进程 就杀掉全部 worker ？
        if os.getppid() == 1 or os.getpgrp() != os.getpid():
            self.logger.info("graceful stop of workers")
            self.kill_workers(True)
        else:
            self.log.info("SIGWINCH ignored. not daemonized")
    
    def wakeup(self):
        # Wake up the arbiter
        try:
            # 怎么唤醒？？？
            os.write(self.PIPE[1], '.')
        except IOError, e:
            # EAGAIN try again
            # EINTR Interrupted system call

            # Many system calls will report the EINTR error code 
            # if a signal occurred while the system call was in progress. 
            # No error actually occurred, it's just reported that way 
            # because the system isn't able to resume the system call automatically. 
            # This coding pattern simply retries the system call when this happens, to ignore the interrupt.

            # For instance, this might happen if the program makes use of alarm()
            #  to run some code asynchronously when a timer runs out. 
            # If the timeout occurs while the program is calling write(), 
            # we just want to restart it.
            if e.errno not in [errno.EAGAIN, errno.EINTR]:
                raise
                    
    def sleep(self):
        try:
            # 管道里是否有数据，等 1s
            ready = select.select([self.PIPE[0]], [], [], 1.0)
            # 下面两种方式有什么区别？？
            if not ready[0]:
                return
            while os.read(self.PIPE[0], 1):
                pass
        except select.error, e:
            if e[0] not in [errno.EAGAIN, errno.EINTR]:
                raise
        except OSError, e:
            if e.errno not in [errno.EAGAIN, errno.EINTR]:
                raise
        except KeyboardInterrupt:
            sys.exit()
    
    def stop(self, graceful=True):
        # 清理监听的 socket
        self.LISTENER = None
        # 来自键盘的退出
        sig = signal.SIGQUIT
        if not graceful:
            # 软件终止信号
            sig = signal.SIGTERM
        # 
        limit = time.time() + self.timeout
        while len(self.WORKERS) and time.time() < limit:
            self.kill_workers(sig)
            time.sleep(0.1)
            # 回收 worker 避免僵尸进程
            self.reap_workers()
        # 超过给定时间还没有干掉的就强杀了？
        self.kill_workers(signal.SIGKILL)
        
    def reexec(self):
        # fork 一份？为什么？？ 为什么收到信号重新起一个程序？？
        self.reexec_pid = os.fork()
        if self.reexec_pid == 0:
            # 将监听的 socket id 保存的环境变量里
            os.environ['GUNICORN_FD'] = str(self.LISTENER.fileno())
            # 重新运行本程序？？，然后继续监听原来的 socket
            os.execlp(sys.argv[0], *sys.argv)

    def murder_workers(self):
        # 谋杀 worker 为什么？？

        # struct stat {
        #     dev_t     st_dev;     /* ID of device containing file */
        #     ino_t     st_ino;     /* inode number */
        #     mode_t    st_mode;    /* protection */
        #     nlink_t   st_nlink;   /* number of hard links */
        #     uid_t     st_uid;     /* user ID of owner */
        #     gid_t     st_gid;     /* group ID of owner */
        #     dev_t     st_rdev;    /* device ID (if special file) */
        #     off_t     st_size;    /* total size, in bytes */
        #     blksize_t st_blksize; /* blocksize for file system I/O */
        #     blkcnt_t  st_blocks;  /* number of 512B blocks allocated */
        #     time_t    st_atime;   /* time of last access */
        #     time_t    st_mtime;   /* time of last modification */
        #     time_t    st_ctime;   /* time of last status change */
        # };
        
        for (pid, worker) in list(self.WORKERS.items()):
            # 这里有个问题，这个临时文件最近一次状态改变的时间究竟意味着什么我得去看看才知道，最好有一个封装告诉我是啥。
            # worker.xxxtime?

            # The field st_ctime is changed by writing or by setting inode information 
            # (i.e., owner, group, link count, mode, etc.).
            diff = time.time() - os.fstat(worker.tmp.fileno()).st_ctime
            if diff <= self.timeout:
                continue
            self.log.error("worker %s PID %s timeout killing." % (str(worker.id), pid))
        # 超时了，还没死就直接强杀， worker 不要跑太久。。。
            self.kill_worker(pid, signal.SIGKILL)
    
    def reap_workers(self):
        # 收割 worker？？
        try:
            while True:
                wpid, status = os.waitpid(-1, os.WNOHANG)
                if not wpid: break
                if self.reexec_pid == wpid:
                    self.reexec_pid = 0
                else:
                    worker = self.WORKERS.pop(wpid, None)
                    if not worker:
                        continue
                    # close 干嘛？？ worker 是不是还活着
                    worker.tmp.close()
        except OSError, e:
            # No child processes
            if e.errno == errno.ECHILD:
                pass
    
    def manage_workers(self):
        # worker 少了就加
        if len(self.WORKERS.keys()) < self.num_workers:
            self.spawn_workers()

        # worker 多了就关掉
        for pid, w in self.WORKERS.items():
            if w.id >= self.num_workers:
                self.kill_worker(pid, signal.SIGQUIT)

    def spawn_workers(self):
        workers = set(w.id for w in self.WORKERS.values())
        for i in range(self.num_workers):
            if i in workers:
                continue
            
            # 监听的 socket 给 worker
            worker = Worker(i, self.pid, self.LISTENER, self.modname,
                        self.timeout)
            pid = os.fork()
            if pid != 0:
                self.WORKERS[pid] = worker
                continue
            
            # Process Child
            worker_pid = os.getpid()
            try:
                self.log.info("Worker %s booting" % worker_pid)
                worker.run()
                sys.exit(0)
            except SystemExit:
                
                raise
            except:
                self.log.exception("Exception in worker process.")
                sys.exit(-1)
            finally:
                worker.tmp.close()
                self.log.info("Worker %s exiting." % worker_pid)

    def kill_workers(self, sig):
        for pid in self.WORKERS.keys():
            self.kill_worker(pid, sig)
        
    def kill_worker(self, pid, sig):
        worker = self.WORKERS.pop(pid)
        try:
            os.kill(pid, sig)
            # 避免僵尸进程，
            # os.WNOHANG 表示如果没有可用的需要 wait 退出状态的子进程，立即返回不阻塞
            kpid, stat = os.waitpid(pid, os.WNOHANG)
            if kpid:
                self.log.warning("Problem killing process: %s" % pid)
        except OSError, e:
            # errno.ESRCH No such process
            if e.errno == errno.ESRCH:
                try:
                    worker.tmp.close()
                except:
                    pass

