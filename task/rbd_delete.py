#!/usr/bin/env python
# coding=UTF-8

from lib.task import Task


class RBDDeleteTask(Task):
    def __init__(self, conf_file, keyring_file, cluster_name, pool_name, rbd_name):
        super(RBDDeleteTask, self).__init__()

        self.conf_file = conf_file
        self.keyring_file = keyring_file
        self.cluster_name = cluster_name
        self.pool_name = pool_name
        self.rbd_name = rbd_name
        self.conf_file = conf_file

        self.name = self.__str__()

    def __str__(self):
        return "delete_rbd_%s/%s" %(self.pool_name,
                                    self.rbd_name)

    def get_command(self):
        try:
            self.command = "rbd rm --no-progress --conf %s --keyring %s --cluster %s " \
                           "--pool %s %s" % (self.conf_file,
                                             self.keyring_file,
                                             self.cluster_name,
                                             self.pool_name,
                                             self.rbd_name)
            return self.command
        except Exception as e:
            self.error = e
            return False
