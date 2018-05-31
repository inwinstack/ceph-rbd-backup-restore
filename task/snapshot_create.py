#!/usr/bin/env python
# coding=UTF-8
# Author: Yu-Jung Cheng

import datetime

from lib.task import Task
from lib.common_func import *


class SnapshotCreateTask(Task):
    def __init__(self, conf_file, keyring_file, cluster_name, pool_name, rbd_name,
                 snap_name=None, snap_datetime_fmt=None):
        super(SnapshotCreateTask, self).__init__()

        self.conf_file = conf_file
        self.keyring_file = keyring_file
        self.cluster_name = cluster_name
        self.pool_name = pool_name
        self.rbd_name = rbd_name
        self.snap_name = snap_name
        self.snap_datetime_fmt = snap_datetime_fmt

        self.name = self.__str__()
        self.create_datetime = None

    def __str__(self):
        if self.snap_name == None:
            return "create_snapshot_%s/%s" %(self.pool_name, self.rbd_name)
        return "create_snapshot_%s/%s@%s" %(self.pool_name, self.rbd_name, self.snap_name)

    def get_command(self):
        try:
            if self.snap_datetime_fmt == None:
                new_datetime = get_datetime()
                self.create_datetime = normalize_datetime(new_datetime)
            else:
                self.create_datetime = get_datetime(str_format=self.snap_datetime_fmt)

            if self.snap_name is None:
                self.snap_name = self.create_datetime

            self.command = "rbd snap create --conf %s --keyring %s --cluster %s " \
                           "--pool %s %s@%s" % (self.conf_file,
                                                self.keyring_file,
                                                self.cluster_name,
                                                self.pool_name,
                                                self.rbd_name,
                                                self.snap_name)
            return self.command
        except Exception as e:
            self.error = e
            return False

    def get_created_snapshot(self):
        return self.snap_name
