#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Yu-Jung Cheng

import subprocess
import time
import os

from multiprocessing import Process
from threading import Thread


class Worker(Process):
    def __init__(self, log, task_queue, finish_queue, pid_queue, rest_time,
                 stop_signal):

        Process.__init__(self)
        self.log = log
        self.task_queue = task_queue
        self.finish_queue = finish_queue
        self.pid_queue = pid_queue

        self.rest_time = rest_time
        self.stop_signal = stop_signal

        self.task_get_count = 0
        self.task_done_count = 0

        self.pid_flags = ['start', 'end', 'io']

        self.monitor = False
        self.print_cmd = False

        self.status = 'ready'   # other status 'wait', 'stop', 'run', 'rest'
        self.log.debug("%s is initialized, rest time %s seconds, status %s." %
                      (self.name, self.rest_time, self.status))

    def _get_io(self, pid, interval=10):
        try:
            self.log.debug("%s starts IO monitoring from pid '%s'." % (self.name, pid))
            pid_io_file = "/proc/%s/io" % pid

            time.sleep(interval)
            while True:
                if not os.path.isfile(pid_io_file):
                    break

                timestamp = time.time()
                with open(pid_io_file, 'r') as iofp:
                    lines = iofp.read().splitlines()
                    r_bytes = int(lines[4].replace('read_bytes: ', ''))
                    w_bytes = int(lines[5].replace('write_bytes: ', ''))
                    io_info = (self.pid_flags[2],
                               self.name,
                               timestamp,
                               pid,
                               r_bytes,
                               w_bytes)
                    self.pid_queue.put(io_info)
                time.sleep(interval)

        except Exception as e:
            self.log.error("%s has IO monitoring error occur, %s" % (self.name, e))
        finally:
            self.log.debug("%s finishd IO monitoring." % self.name)

    def _exec_task(self, task):
        try:
            cmd = task.get_command()
            if cmd == False:
                return 1, "fail to get command.", task.error
            if self.print_cmd:
                print("- %s: %s" % (self.name, cmd))

            task.start_time = time.time()

            self.log.debug("%s is executing command '%s'" % (self.name, cmd))
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            self.log.debug("%s spawns child process pid '%s'." % (self.name, p.pid))

            if hasattr(task, 'monitor'):
                if task.monitor:
                    monitor = Thread(target=self._get_io, args=(p.pid,))
                    monitor.start()

            task.pid = p.pid

            task_pid_info = (self.pid_flags[0],
                             self.name,
                             task.start_time,
                             p.pid,
                             task.name,
                             cmd)
            self.pid_queue.put(task_pid_info)

            output, error = p.communicate()
            return_code = p.returncode

            task.end_time = time.time()

            task_pid_info = (self.pid_flags[1],
                             self.name,
                             task.end_time,
                             p.pid,
                             task.name,
                             return_code)
            self.pid_queue.put(task_pid_info)

            return return_code, output, error
        except Exception as e:
            self.log.error("%s failed to execute command '%s'. %s" %
                          (self.name, cmd, e))
            return 1, "Exception", e

    def run(self):
        self.log.debug("%s start running, pid = '%s'" % (self.name, self.pid))
        while True:
            task = None

            try:
                self.status = 'wait'
                self.log.debug("%s is waiting for new task." % (self.name))
                task = self.task_queue.get()

                if task is self.stop_signal:
                    self.log.debug("%s is going to be stopped." % self.name)
                    self.status = 'stop'
                    self.task_queue.task_done()
                    break

                self.log.debug("%s got task '%s'" % (self.name, task))
                self.status = 'run'
                self.task_get_count += 1

                task.worker_name = self.name

                return_code, output, error = self._exec_task(task)
                task.return_code = return_code
                task.output = output
                task.error = error

                elapsed_time = task.get_elapsed_time()
                self.log.debug("%s finished executing task '%s', " \
                               "elapsed time %s." %(self.name, task, elapsed_time))

                self.finish_queue.put(task)
                self.task_queue.task_done()
                self.task_done_count += 1
                self.status = 'rest'
                time.sleep(self.rest_time)
            except Exception as e:
                # if error occur, just move on next task...
                self.log.error("%s occur exception. task name = %s, %s" %(self.name, task, e))
                self.finish_queue.put(task)
                continue

        self.log.debug("%s is stopped." % (self.name))
        return True
