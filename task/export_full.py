#!/usr/bin/env python
# coding=UTF-8

import os

from lib.common_func import *
from lib.task import Task


class ExportFullTask(Task):
    def __init__(self, conf_file, keyring_file, cluster_name, pool_name, rbd_name,
                 snap_name=None, dest_path=None, dest_file=None,
                 export_datetime_fmt=None):
        super(ExportFullTask, self).__init__()

        self.conf_file = conf_file
        self.keyring_file = keyring_file
        self.cluster_name = cluster_name
        self.pool_name = pool_name
        self.rbd_name = rbd_name
        self.dest_path = dest_path
        self.dest_file = dest_file
        self.snap_name = snap_name
        self.export_datetime_fmt = export_datetime_fmt

        self.name = self.__str__()

        self.dest_filepath = ""
        self.export_type = 'full'
        self.monitor = True
        self.create_datetime = None

    def __str__(self):
        if self.snap_name == None:
            return "full_export_%s/%s" %(self.pool_name, self.rbd_name)
        else:
            return "full_export_%s/%s@%s" %(self.pool_name,
                                            self.rbd_name,
                                            self.snap_name)

    def get_command(self):
        try:
            if self.snap_name == None:
                rbd_name = self.rbd_name
                if self.export_datetime_fmt == None:
                    new_datetime = get_datetime()
                    self.create_datetime = normalize_datetime(new_datetime)
                else:
                    self.create_datetime = get_datetime(str_format=self.export_datetime_fmt)
            else:
                rbd_name = "%s@%s" % (self.rbd_name, self.snap_name)
                self.create_datetime = self.snap_name

            if self.dest_path == None:
                self.dest_path = "./"

            if self.dest_file == None:
                self.dest_file = self.create_datetime
                if self.snap_name != None:
                    self.dest_file = self.snap_name

            self.dest_filepath = os.path.join(self.dest_path,
                                              self.dest_file)

            self.command = "rbd export --no-progress --conf %s --keyring %s " \
                           "--cluster %s --pool %s %s %s " % (self.conf_file,
                                                              self.keyring_file,
                                                              self.cluster_name,
                                                              self.pool_name,
                                                              rbd_name,
                                                              self.dest_filepath)

            #from random import randint
            #sec = randint(30, 60)
            #self.command = "%s ; sleep %s" % (self.command, sec)

            return self.command
        except Exception as e:
            self.error = e
            return False

    def get_export_circle_name(self):
        if self.dest_path[-1] == '/':
            return os.path.basename(self.path[:-1])
        else:
            return os.path.basename(self.path)
