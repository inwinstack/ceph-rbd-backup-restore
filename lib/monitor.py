#!/usr/bin/env python
# coding=UTF-8
# Author: Yu-Jung Cheng

import os
import time
import datetime
import subprocess

from multiprocessing import Process, Queue
from threading import Thread


# unused
class Monitor(Process):

    def __init__(self, log, monitor_queue, interval=10):
        Process.__init__(self)
        self.log = log
        self.monitor_queue = monitor_queue
        self.interval = interval

        self.thread_counter = 0
        self.thread_stop = False

    def _exec_cat(self, cmd):
        try:
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            output, error = p.communicate()
            return_code = p.returncode
            return return_code, output, error
        except Exception as e:
            return False

    def _disk_io(self, worker_name, pid):
        timestamp = time.time()
        initial_value = (0, 0, timestamp)
        try:
            def parse_io_value(data):
                data_line = data.split('\n')
                r_value = int(data_line[4].replace('read_bytes: ', ''))
                w_value = int(data_line[5].replace('write_bytes: ', ''))
                return r_value, w_value

            self.log.debug("monitor disk IO of pid '%s' for worker '%s'." %
                          (pid, worker_name))

            pid_io_path = "/proc/%s/io" % pid
            cmd = "cat %s 2>/dev/null" % (pid_io_path)
            rc, data, err = self._exec_cat(cmd)
            if rc == 0:
                r_bytes, w_bytes = parse_io_value(data)
                initial_value = (r_bytes, w_bytes, timestamp)
            time.sleep(self.interval)

            while True:
                if not os.path.isfile(pid_io_path):
                    break
                rc, data, err = self._exec_cat(cmd)
                timestamp = time.time()
                if rc == 0:
                    interval = int(timestamp - initial_value[2])
                    r_bytes, w_bytes = parse_io_value(data)
                    r_speed = int((r_bytes - initial_value[0]) / interval / 1024)
                    w_speed = int((w_bytes - initial_value[1]) / interval / 1024)
                    self.log.debug("pid:%s  ts:%s  w_bytes:%s  r_bytes:%s  w:%s KB/s  r:%s KB/s" %
                                  (pid, timestamp, w_bytes, r_bytes, w_speed, r_speed))
                    initial_value = (r_bytes, w_bytes, timestamp)
                else:
                    break    # maybe the pid is done, so finish this monitoring
                time.sleep(self.interval)
        except Exception as e:
            self.log.debug("pid:%s error occur, %s." % (pid, e))

    def run(self):
        self.log.debug("---- start running monitor ----")
        while True:
            pid_info = self.monitor_queue.get()
            if pid_info == None:
                self.log.debug("---- stop runnning monitor ----")
                break
            worker_name = pid_info[0]
            pid = pid_info[1]
            t_disk_io = Thread(target=self._disk_io, args=(worker_name, pid,))
            t_disk_io.start()
            self.thread_counter += 1

    def stop(self):
        self.monitor_queue.put(None)
