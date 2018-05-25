#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import os

from multiprocessing import Queue, JoinableQueue
from threading import Thread

from lib.logger import Logger
from lib.worker import Worker
from lib.monitor import Monitor


class Manager(object):
    def __init__(self, log, worker_count=1, rest_time=1, stop_signal=None,
                            tmp_dir='/tmp'):
        self.log = log
        self.worker_count = int(worker_count)
        self.rest_time = rest_time
        self.stop_signal = stop_signal
        self.tmp_dir = tmp_dir

        self.monitoring = False
        self.monitor = None
        self.workers = {}
        self.workers_status = {}
        self.workers_pid = {}

        self.worker_log = None
        self.monitor_log = None

        self.task_queue = JoinableQueue()
        self.finished_task_queue = Queue()
        self.pid_queue = Queue()
        self.monitor_queue = Queue()

        self.pid_start_flag = 'start'
        self.pid_end_flag = 'end'
        self.pid_watcher = Thread(target=self._pid_queue_watcher_thread)
        self.pid_watcher.start()

        self.log.debug("Worker manager initialized, set %s workers." % worker_count)

    def _pid_queue_watcher_thread(self):
        try:
            tmp_dir = os.path.join(self.tmp_dir, 'worker_pid_status')
            os.system("mkdir -p %s" % tmp_dir)
            while True:
                pid_info = self.pid_queue.get()
                if pid_info == None:
                    self.log.debug("Manage stop watching pid queue thread.")
                    break

                pid_info_flag = pid_info[0]
                worker_name = pid_info[1]
                timestamp = pid_info[2]
                pid_number = pid_info[3]
                task_name = pid_info[4]

                shm_worker_file = "%s/%s" % (tmp_dir, worker_name)
                shm_pid_file = "%s/%s" % (tmp_dir, pid_number)

                if pid_info_flag == self.pid_start_flag:
                    task_command = pid_info[5]
                    worker_status = "%s %s %s" % (pid_number, task_name, task_command)
                    os.system("echo '%s' > %s" % (worker_status, shm_worker_file))
                    pid_status = "%s %s %s %s" % (timestamp, task_name, worker_name, task_command)
                    os.system("echo '%s' > %s" % (pid_status, shm_pid_file))

                elif pid_info_flag == self.pid_end_flag:
                    return_code = pid_info[5]
                    pid_status = "%s %s" % (timestamp, return_code)
                    os.system("echo '%s' >> %s" % (pid_status, shm_pid_file))
                    os.system("echo '' > %s" % shm_worker_file)

                if self.monitoring:
                    if pid_info_flag == self.pid_start_flag:
                        self.monitor_queue.put((worker_name, pid_number))

            #os.system("rm -rf %s" % tmp_dir)
        except Exception as e:
            self.log.error("Manager failed to start pid watcher. %s" % e)
            return False

    def set_worker_logger(self, file_path, name="worker",
                                           level="DEBUG",
                                           max_bytes=20971520,
                                           backup_count=10,
                                           delay=False):
        self.log.debug("Set worker logger.")
        logger = Logger(file_path, level, max_bytes, backup_count, delay, name=name)
        logger.set_log(log_format='[%(asctime)s] %(message)s')
        self.worker_log = logger.get_log()

    def set_monitor_logger(self, file_path, name="monitor",
                                            level="DEBUG",
                                            max_bytes=20971520,
                                            backup_count=10,
                                            delay=False):
        self.log.debug("Set monitor logger.")
        logger = Logger(file_path, level, max_bytes, backup_count, delay,  name=name)
        logger.set_log(log_format='[%(asctime)s] %(message)s')
        self.monitor_log = logger.get_log()

    def init_monitor(self, interval=10):
        try:
            self.log.debug("Manager initializes monitor.")

            if self.monitor_log == None:
                self.log.debug("Set monitor logger to backup logger.")
                self.monitor_log = self.log
            monitor = Monitor(self.monitor_log,
                              self.monitor_queue,
                              interval=int(interval))
            self.monitor = monitor
        except Exception as e:
            self.log.error("Manager failed to initialize monitor. %s" % e)
            return False

    def run_monitor(self):
        if self.monitoring == False:
            self.log.debug("Manager starts monitor.")
            self.monitor.start()
            self.monitoring = True

    def stop_monitor(self):
        if self.monitoring == True:
            self.log.debug("Manager stops monitor.")
            self.monitor.stop()
            self.monitoring = False

    def run_worker(self, count=None):
        try:
            self.log.debug("Manager starts runing %s workers." % self.worker_count)

            if self.worker_log == None:
                self.log.debug("Set worker logger to manager's logger.")
                self.worker_log = self.log

            if count != None:
                self.worker_count = int(count)

            workers = [ Worker(self.worker_log,
                               self.task_queue,
                               self.finished_task_queue,
                               self.pid_queue,
                               self.rest_time,
                               self.stop_signal,
                               pid_start_flag=self.pid_start_flag,
                               pid_end_flag=self.pid_end_flag)
                        for i in xrange(self.worker_count) ]

            for worker in workers:
                worker.start()
                self.workers[worker.name] = worker
                self.workers_pid[worker.name] = worker.pid
                self.log.debug("%s PID = %s" % (worker.name, worker.pid))

            time.sleep(1)

            return True
        except Exception as e:
            self.log.error("Manager failed to run workers. %s" % e)
            return False

    # call after all tasks are done
    def stop_worker(self, count=0):
        try:
            if self.worker_count == 0:
                return True

            # check number of worker to stop
            if count == 0 or count >= self.worker_count:
                self.log.debug("Manager stops all workers")
                stop_count = int(self.worker_count)
            else:
                self.log.debug("Manager stops %s workers" % count)
                stop_count = int(count)

            for count in range(0, stop_count):
                self.log.debug("Manager sents a stop singal to workers. count = %s." % (count+1))
                self.task_queue.put(self.stop_signal)

            self.worker_count -= 1
        except Exception as e:
            self.log.error("Manager failed to stop worker. %s" % e)
            return False

    def add_task(self, task):
        try:
            self.log.debug("Manager receives task '%s'." % task)
            if task == False or task == None:
                self.log.error("Manager receives invalid task.")
                return False
            self.task_queue.put(task)
            return True
        except Exception as e:
            self.log.error("Manager failed adding task '%s'. %s" % (task,e))
            return False

    def get_finished_task(self):
        return self.finished_task_queue.get()

    def get_workers_status(self):
        for name, worker in self.workers.iteritems():
            self.workers_status[name] = worker.status
        return self.workers_status

    def kill_worker(self):
        for worker_name, worker_pid in self.workers_pid.iteritems():
            cmd = "kill -9 %s" % worker_pid
            os.system(cmd)
            self.log.debug("Kill worker '%s', pid = %s" % worker_name,
                                                               worker_pid)
        return True

    def stop(self):
        self.pid_queue.put(None)
        self.stop_worker()
        self.stop_monitor()
