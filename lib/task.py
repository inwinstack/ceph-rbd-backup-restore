#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Yu-Jung Cheng

import time, datetime, collections


class Task(object):
    def __init__(self):
        self.init_time = time.time()
        self.start_time = 0
        self.end_time = 0
        self.elapsed_time = 0

        self.name = "task_%s" % int(self.init_time)
        self.command = ""
        self.worker_name = ""
        self.return_code = ""
        self.output = ""
        self.error = ""
        self.pid = ""
        self.monitor = False

    def __str__(self):
        return self.name

    def get_command(self):
        return self.command

    def get_elapsed_time(self):
        try:
            if self.start_time == 0 or self.end_time == 0:
                return False
            else:
                if self.start_time > self.end_time:
                    return False
                else:
                    self.elapsed_time = self.end_time - self.start_time
                    return self.elapsed_time
        except Exception as e:
            self.error += "; elapsed_time_error = %s" % e
            return False

    def get_result(self):
        od = collections.OrderedDict()
        od['task_name'] = self.__str__()
        od['worker_name'] = self.worker_name
        od['init_time'] = self.init_time
        od['start_time'] = self.start_time
        od['end_time'] = self.end_time
        od['elapsed_time'] = self.elapsed_time
        od['command'] = self.command
        od['return_code'] = self.return_code
        od['output'] = self.output
        od['error'] = self.error
        od['pid'] = self.pid
        return od
