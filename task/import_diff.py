#!/usr/bin/env python
# coding=UTF-8

import os

from lib.task import Task


class ImportDiffTask(Task):
    def __init__(self, conf_file, keyring_file, cluster_name, pool_name, rbd_name,
                 src_path, src_file):
        super(ImportDiffTask, self).__init__()

        self.conf_file = conf_file
        self.keyring_file = keyring_file
        self.cluster_name = cluster_name
        self.pool_name = pool_name
        self.rbd_name = rbd_name
        self.src_path = src_path
        self.src_file = src_file

        self.name = self.__str__()

        self.src_filepath = os.path.join(self.src_path, self.src_file)
        self.import_type = 'diff'
        self.monitor = True

    def __str__(self):
        return "diff_import_%s/%s_from_%s" % (self.pool_name,
                                              self.rbd_name,
                                              self.src_file)

    def get_command(self):
        try:
            self.command = "rbd import-diff --no-progress --conf %s --keyring %s " \
                           "--cluster %s --pool %s %s %s" % (self.conf_file,
                                                             self.keyring_file,
                                                             self.cluster_name,
                                                             self.pool_name,
                                                             self.src_filepath,
                                                             self.rbd_name)
            return self.command
        except Exception as e:
            self.error = e
            return False
