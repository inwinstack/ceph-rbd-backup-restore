#!/usr/bin/env python
# coding=UTF-8
# Author: Yu-Jung Cheng

from lib.task import Task


class SnapshotDeleteTask(Task):
    def __init__(self, conf_file, keyring_file, cluster_name, pool_name, rbd_name, snap_name):
        super(SnapshotDeleteTask, self).__init__()

        self.conf_file = conf_file
        self.keyring_file = keyring_file
        self.cluster_name = cluster_name
        self.pool_name = pool_name
        self.rbd_name = rbd_name
        self.snap_name = snap_name
        self.conf_file = conf_file

        self.name = self.__str__()

    def __str__(self):
        return "delete_snapshot_%s/%s@%s" %(self.pool_name,
                                            self.rbd_name,
                                            self.snap_name)

    def get_command(self):
        try:
            self.command = "rbd snap rm --no-progress --conf %s --keyring %s --cluster %s " \
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
